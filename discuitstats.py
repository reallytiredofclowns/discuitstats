import requests, time, pandas, datetime

# URL of the last report, to link back to it in the current report
lastReportURL = "https://discuit.net/DiscuitMeta/post/m613rjsc"
# set fromDate to "" to get all
fromDate = "20240816"
toDate = "20240823"

# summary tables show top X items
topX = 10

# no point calculating stats for bots
ignoredUsers = ["autotldr", "FlagWaverBot", "Betelgeuse", "catbot"]

# initial feed nextPage parameter--to be used in eventual resumption code
nextPage = ""

baseURL = "https://discuit.net"
#baseURL = "http://localhost:8080"

dateTimeRestart = None

##########################################################

# convert string server datetime to "YYYYMMDD" format
def dateFormat(date):
  return date[:10].replace("-", "")

# convert string server datetime to Python datetime
def serverDateToDT(s):
  serverDateFormat = '%Y-%m-%dT%H:%M:%S%z'
  return datetime.datetime.strptime(s, serverDateFormat)

# convert string server datetime to nanosecond, suitable for use with
# comparing last activity pagination
def serverDateToNS(s):
  return int(serverDateToDT(s).timestamp() * 10**9)

def daysAgo(dt):
  currDateTime = datetime.datetime.now(tz=datetime.timezone.utc)
  return max(0, (currDateTime - dt).days)

# title field may have special characters that need to be escaped
def cleanTitle(title):
  return title.translate(str.maketrans({
    "|": r"\|", "[": r"\[", "]": r"\]", "(": r"\(", ")": r"\)",
    "_": r"\_", "*": "\*"}))

def fetchFeed(feedNext, disc = None, sort = "activity"):
  args = {"sort": sort, "next": feedNext}
  if disc:
    args["communityId"] = disc
  response = requests.get(rf"{baseURL}/api/posts", args)
  json = response.json()
  return json["posts"], json["next"]

def getFullPost(post):
  return requests.get(
    f"{baseURL}/api/posts/{post['publicId']}").json()

def commentIsValid(comment, rawData, postCommentId):
  if postCommentId in rawData.index:
    return True
  if comment["deletedAt"]:
    return False
  if comment["editedAt"]:
    commentDate = dateFormat(comment["editedAt"])
  else:
    commentDate = dateFormat(comment["createdAt"])
  if (fromDate != "" and commentDate < fromDate) or commentDate > toDate:
    return False
  return True

def processComments(post, rawData, publicId, discName):
  # posts from home feed don't seem to contain comments
  fullPost = getFullPost(post)
  comments = fullPost["comments"]
  commentsNext = fullPost["commentsNext"]
  anyCommentValid = False
  while comments:
    for comment in comments:
      postCommentId = publicId + "/" + comment["id"]
      if not commentIsValid(comment, rawData, postCommentId):
        continue
      anyCommentValid = True
      postCommentId = publicId + "/" + comment["id"]
      if not postCommentId in rawData:
        rawData.loc[postCommentId, ["Type", "Disc", "User", "PublicId", "IsBot"]] =\
          ["Comment", discName, comment["username"], publicId, comment["username"] in ignoredUsers]
    if commentsNext:
      comments = requests.get(
        f"{baseURL}/api/posts/{publicId}/comments",
        {"next": commentsNext}).json()
      comments, commentsNext = comments["comments"], comments["next"]
    else:
      break
  return anyCommentValid

# A post can have dates that are out of range, but if its
# comments are in the date range, they need to be counted.
# So even in the primary scan, before the rescan, should examine the comments
# in posts with last activity > toDate, because they could have been
# bumped.
def processPosts(posts, rawData, isRescan = False):
  reachedTimeLimit = False
  lastSuccessfulPostDate = ""
  for post in posts:
    anyCommentValid = False
    validPost = False
    lastActivityAt = dateFormat(post["lastActivityAt"])
    createdAt = dateFormat(post["createdAt"])
    publicId = post["publicId"]
    discName = post["communityName"]
    # server dates should be NNNNNN format, so coerce a blank toDate to "z"
    # to simplify the comparisons
    validPostDate = (fromDate <= createdAt <= (toDate or "z")) or\
      (fromDate <= lastActivityAt <= (toDate or "z"))
    if fromDate != "" and lastActivityAt < fromDate:
      reachedTimeLimit = True
      break
    if post["noComments"]:
      anyCommentValid = processComments(post, rawData, publicId, discName)
    validPost = (anyCommentValid or validPostDate or publicId in rawData.index)
    # needs to overwrite during rescan, to pick up the last activity time
    if validPost:
      username = post["username"]
      title = cleanTitle(post["title"].replace("\n", " "))
      postType = post["type"].title() # "text", "image", "link"
      lastActivityRaw = serverDateToDT(post["lastActivityAt"])
      rawData.loc[
        publicId,
        ["Type", "Disc", "Title", "User", "PublicId", "LastActivity", "IsBot", "CreateDate"]] =\
        [postType, discName, title, username, publicId, lastActivityRaw, username in ignoredUsers, createdAt]
    lastSuccessfulPostDate = lastActivityAt
  return lastSuccessfulPostDate, reachedTimeLimit

#####################################################################
# Functions for rescanning activity feed after the main loop has completed.
# Necessary because while doing the main loop, users could have bumped
# old posts up in the feed, and the main loop would not fetch them,
# as their activity has been resorted to the top of the feed.

# helper function to update store of posts to rescan
def updateRedos(publicIds, posts, rawData):
  for post in posts:
    publicId = post["publicId"]
    activity = post["lastActivityAt"]
    activityDT = serverDateToDT(activity)
    # if a post was created after the date limit, comments cannot be in range
    if dateFormat(post["createdAt"]) > toDate:
      continue
    if publicId in publicIds and publicIds[publicId]["lastActivityAt"] == activity:
      # if the post is in the redo set and its last activity is the same
      # as what has been seen in the rescanning so far, no need to update
      continue
    if publicId in rawData.index and activityDT == rawData.loc[publicId]["LastActivity"]:
      # if the current post last activity is equal to what was recorded
      # in the main loop, there is no change, so skip
      continue
    # otherwise, the post needs its comments rescanned
    # don't have to update with changed post data, since we're only interested
    # in the IDs to reexamine the comments
    publicIds[publicId] = post

# rescan from the top of the activity feed to a given latest nanosecond pagination
def rescan(latestDate, publicIds, rawData):
  nextPage = ""
  firstIter = True
  while True:
    print(f"Collecting bumped activity after main loop... nextPage = {nextPage} "
          f"with {len(publicIds)} posts in the rescan set")
    posts, nextPage = fetchFeed(nextPage)
    if firstIter:
      firstIter = False
      if posts:
        # save details of the top item of the feed for later, to determine
        # if the item was already seen, and therefore nothing changed
        # and we can stop looping
        scanFirstDate = serverDateToNS(posts[0]["lastActivityAt"])
        scanFirstPublicId = posts[0]["publicId"]
      else:
        scanFirstDate = None
        scanFirstPublicId = None
    updateRedos(publicIds, posts, rawData)
    time.sleep(2)
    # stop loop if the pagination is earlier
    if nextPage is None or int(nextPage) < latestDate:
      break
  return scanFirstPublicId, scanFirstDate

def getRedoPosts(latestDate, rawData):
  publicIds = dict()
  prevDate = None
  prevPublicId = None
  firstIter = True
  # if the first post in the current redo scan has the same id/last activity
  # as the first post in the previous scan, then the feed has not changed
  # and we're done rescanning
  while True:
    scanFirstPublicId, scanFirstDate = rescan(latestDate, publicIds, rawData)
    latestDate = scanFirstDate
    if scanFirstDate is None:
      break # this should mean the feed is empty
    if firstIter:
      firstIter = False
    else:
      # not the first iteration: check to see if the first item in the feed
      # is unchanged--if so, done rescanning
      if scanFirstPublicId == prevPublicId and scanFirstDate == prevDate:
        break
    prevPublicId = scanFirstPublicId
    prevDate = scanFirstDate
  return list(publicIds.values())

#####################################################################

def generateTables(nextPage):
  lastPostDate = ""
  rawData = pandas.DataFrame({
    "Type": [], "Disc": [], "Title": [], "User": [], "PublicId": [],
    "LastActivity": [], "IsBot": [], "CreateDate": []})
  while True:
    print(f"Pagination parameter is: {nextPage}; last processed post date was: {lastPostDate}")
    posts, nextPage = fetchFeed(nextPage)
    lastPostDate, reachedTimeLimit = processPosts(
      posts, rawData)
    if nextPage is None or reachedTimeLimit:
      break
    time.sleep(2)

  # need to check for posts that were bumped during looping
  print("Relooping to search for posts that were bumped")
  latestDate = int(
    rawData.query("Type != 'Comment'")["LastActivity"].max().timestamp()
    * 10**9)
  # get a list of posts to recheck
  redoPosts = getRedoPosts(latestDate, rawData)
  # process the rescans in chunks so as not to overwhelm the site
  start = 0
  while True:
    nextPosts = redoPosts[start:start + 10]
    if nextPosts:
      processPosts(nextPosts, rawData, isRescan = True)
      start += 10
    else:
      break
    time.sleep(2)
  return rawData

def topXReport(rawData):
  rawData["IsBot"] = rawData["IsBot"].astype(bool)
  if set(rawData["IsBot"].unique()) != {True, False}:
    print("Something went wrong; rawData's IsBot is not uniquely True/False")
    raise BaseException
  nonBot = rawData[~rawData["IsBot"].astype(bool)]
  sumPostComments = len(nonBot.query("Type == 'Comment'"))
  numDiscs = len(nonBot['Disc'].unique())
  activeUsers = len(nonBot['User'].unique())
  activePosts = len(nonBot['PublicId'].unique())
  # grouping by post's publicId, comment count is total count minus 1 (the post)
  rawData["Comments"] = (
    rawData.groupby("PublicId")["Type"]
      .transform(lambda x: pandas.Series.count(x) - 1))
  print(f"\n\nDiscuit week in review: {fromDate}-{toDate}\n")

  print(f"\n[Last week's report is here]({lastReportURL}).")

  print("\nDiscuit API is [documented here](https://docs.discuit.net/getting-started). "
        "Source code of script generating the tables is "
        "[available here](https://gist.github.com/reallytiredofclowns/b51f63d042a4b5416ceee282ee524295).")

  registeredAccounts = requests.get(
    f"{baseURL}/api/_initial").json()["noUsers"]
  print(f"\n{activeUsers} users discussed {activePosts} posts in "
        f"{sumPostComments} comments over {numDiscs} total discs. "
        f"At the time of this report, there were {registeredAccounts} accounts.\n")

  print("Felix30 has been [charting some of these numbers here](https://docs.google.com/spreadsheets/d/1H7zV_7YIZar9dwDHbutr0Dm9N6H-1mEXe0irIwSHsx0/edit#gid=1256137398).\n")

  postTypes = rawData["Type"][rawData["Type"] != 'Comment'].unique()
  postTypes.sort()
  for postType in postTypes:
    subset = (rawData.query("Type == @postType")
      .drop(columns = ["Type", "PublicId"]).copy())
    if len(subset):
      # this really should be moved to the data capture section
      subset["User"] = subset["User"].str.replace("_", "\\_")
      subset["Rank"] = subset["Comments"].rank(method = "min", ascending = False)
      subset = subset.query("Rank <= @topX")
      subset = subset.sort_values("Rank")
      subset["Title"] = (
        "[" + subset['Title'] + f"]({baseURL}/" + subset['Disc'] +
        "/post/" + subset.index + ")")
      subset = subset[["Rank", "Disc", "Title", "User", "Comments"]]
      print(f"# Top {topX} most engaging {postType}s:")
      print(subset.to_markdown(index = False))
      print("\n\n")

  # disc activity
  subset = rawData.copy()
  # don't count posts created out-of-date-range (could have been included
  # due to comments being in date range)
  deletes = subset[
    (subset["CreateDate"] > toDate) |
    ((fromDate != "") & (subset["CreateDate"] < fromDate))
  ].index
  subset = subset.drop(index = deletes)
  subset["Type"] = subset["Type"] + "s"
  subset = (subset.groupby(["Disc", "Type"], as_index = False)
    .size().pivot(columns = "Type", index = "Disc", values = "size")
    .reset_index().fillna(0))
  subset["TotalPosts"] = subset["Texts"] + subset["Images"] + subset["Links"]
  subset["TotalEngagement"] = subset["TotalPosts"] + subset["Comments"]
  subset["Rank"] = subset["TotalEngagement"].rank(method = "min", ascending = False)
  subset = subset.query("Rank <= @topX")
  subset = subset.sort_values("Rank")
  subset = subset[["Rank", "Disc", "Texts", "Images", "Links", "TotalPosts", "Comments", "TotalEngagement"]]
  subset["Disc"] = "[" + subset["Disc"] + "](" + baseURL + "/" + subset["Disc"] + ")"
  print(f"# Top {topX} most engaging Discs:")
  print(subset.to_markdown(index = False))
  print("\n")

  # user activity--remove Ghost and bot users from the active users table
  subset = rawData.query("(User != 'ghost') & ~IsBot").copy()
  deletes = subset[
    (subset["CreateDate"] > toDate) |
    ((fromDate != "") & (subset["CreateDate"] < fromDate))
  ].index
  subset = subset.drop(index = deletes)
  subset["Type"] = subset["Type"] + "s"
  subset = (subset.groupby(["User", "Type"], as_index = False)
    .size().pivot(columns = "Type", index = "User", values = "size")
    .reset_index().fillna(0))
  subset["TotalPosts"] = subset["Texts"] + subset["Images"] + subset["Links"]
  subset["TotalEngagement"] = subset["TotalPosts"] + subset["Comments"]
  subset["Rank"] = subset["TotalEngagement"].rank(method = "min", ascending = False)
  subset = subset.query("Rank <= @topX")
  subset = subset.sort_values("Rank")
  subset = subset[["Rank", "User", "Texts", "Images", "Links", "TotalPosts", "Comments", "TotalEngagement"]]
  subset["User"] = "[" + subset["User"] + "](" + baseURL + "/@" + subset["User"] + ")"
  print(f"# Top {topX} most engaged Discuiteers:")
  print(subset.to_markdown(index = False))

######################################################

rawData = generateTables(nextPage)
topXReport(rawData)