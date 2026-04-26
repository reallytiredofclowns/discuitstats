import requests, time, pandas, datetime, sys, re

# URL of the last report, to link back to it in the current report
lastReportURL = "/DiscuitMeta/post/UGQ1Enhy"
# set fromDate to "" to get all
fromDate = "20260419"
toDate = "20260426"

reportFileName = None # "d:/docs/download/report_variations2.md" # if not None, will write reports to text file specified

# if command line arguments provided, replace the last report URL and dates
commandLineArgs = sys.argv
if len(commandLineArgs) == 5:
  cmdURL, cmdFrom, cmdTo, cmdReport = commandLineArgs[1:]
  if cmdURL:
    lastReportURL = cmdURL
  if cmdFrom:
    fromDate = cmdFrom
  if cmdTo:
    toDate = cmdTo
  if cmdReport:
    reportFileName = cmdReport

exportCSV = f"d:/docs/download/DiscuitActivity_{fromDate}_{toDate}.csv"

# summary tables show top X items
topX = 10

# no point calculating stats for bots
ignoredUsers = ["autotldr", "FlagWaverBot", "Betelgeuse", "catbot",
                "alttextbot", "DiceBot", "PingBot"]

# for accounts partially controlled by bots and labelled
# dict key is username; value is regular expression to filter comment body
partialBots = {"ILostTheGame": r"^\[BOT\]"}

# initial feed nextPage parameter--to be used in eventual resumption code
nextPage = ""

baseURL = "https://discuit.org"
#baseURL = "http://localhost:8080"


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
    "_": r"\_", "*": r"\*"}))

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
  if (fromDate != "" and commentDate < fromDate) or (commentDate > toDate and toDate):
    return False
  return True


# True/False: does given text match the username's regular expression match
# flagging bot posts of a user partially under bot control
def isPartialBot(username, text):
  if username not in partialBots:
    return False
  if re.compile(partialBots[username], re.I|re.S).search(text):
    return True
  else:
    return False


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
        rawData.loc[
          postCommentId,
          ["Type", "Disc", "Title", "User", "PublicId", "IsBot", "CreateDate",
           "Upvotes", "Downvotes", "CommentBody", "PartialBot"]] =\
          ["Comment", discName, cleanTitle(post["title"].replace("\n", " ")),
           comment["username"], publicId, comment["username"] in ignoredUsers,
           dateFormat(comment["createdAt"]), comment["upvotes"],
           comment["downvotes"], comment["body"],
           isPartialBot(comment["username"], comment["body"])]
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
    # server dates should be NNNNNNNN format, so coerce a blank toDate to "z"
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
      lastActivityRaw = post["lastActivityAt"]
      upvotes = post["upvotes"]
      downvotes = post["downvotes"]
      rawData.loc[
        publicId,
        ["Type", "Disc", "Title", "User", "PublicId", "LastActivity", "IsBot", "CreateDate", "Upvotes", "Downvotes"]] =\
        [postType, discName, title, username, publicId, lastActivityRaw, username in ignoredUsers, createdAt, upvotes, downvotes]
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
    # if a post was created after the date limit, comments cannot be in range
    if dateFormat(post["createdAt"]) > toDate and toDate:
      continue
    if publicId in publicIds and publicIds[publicId]["lastActivityAt"] == activity:
      # if the post is in the redo set and its last activity is the same
      # as what has been seen in the rescanning so far, no need to update
      continue
    if publicId in rawData.index and activity == rawData.loc[publicId]["LastActivity"]:
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
        scanFirstDate = posts[0]["lastActivityAt"]
        scanFirstPublicId = posts[0]["publicId"]
      else:
        scanFirstDate = None
        scanFirstPublicId = None
    updateRedos(publicIds, posts, rawData)
    #time.sleep(2)
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
    latestDate = serverDateToNS(scanFirstDate)
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
    "Type": pandas.Series(dtype = "str"),
    "Disc": pandas.Series(dtype = "str"),
    "Title": pandas.Series(dtype = "str"),
    "User": pandas.Series(dtype = "str"),
    "PublicId": pandas.Series(dtype = "str"),
    "LastActivity": pandas.Series(dtype = "str"),
    "IsBot": pandas.Series(dtype = "bool"),
    "CreateDate": pandas.Series(dtype = "str"),
    "Upvotes": pandas.Series(dtype = "int"),
    "Downvotes": pandas.Series(dtype = "int"),
    "CommentBody": pandas.Series(dtype = "str"),
    "PartialBot": pandas.Series(dtype = "bool")})

  while True:
    print(f"Pagination parameter is: {nextPage}; last processed post date was: {lastPostDate}")
    posts, nextPage = fetchFeed(nextPage)
    lastPostDate, reachedTimeLimit = processPosts(
      posts, rawData)
    if nextPage is None or reachedTimeLimit:
      break
    #time.sleep(2)

  # need to check for posts that were bumped during looping
  print("Relooping to search for posts that were bumped")
  latestDate = serverDateToNS(rawData.query("Type != 'Comment'")["LastActivity"].max())
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
    #time.sleep(2)
  rawData["PartialBot"] = rawData["PartialBot"].fillna(False)
  return rawData


# !!! any point to separating this out as a function if comments/participants
# have to be recalculated?
# def finishData(rawData):
#   rawData = rawData.copy()
#   rawData["IsBot"] = rawData["IsBot"].astype(bool)
#   if not set(rawData["IsBot"].unique()).issubset({True, False}):
#     print("Something went wrong; rawData's IsBot is not uniquely True/False")
#     raise BaseException
#   # grouping by post's publicId, comment count is total count minus 1 (the post)
#   rawData["Comments"] = (
#     rawData.groupby("PublicId")["Type"]
#       .transform(lambda x: pandas.Series.count(x) - 1))
#   rawData["Participants"] = (
#     rawData.groupby("PublicId")["User"]
#       .transform(lambda x: pandas.Series.nunique(x)))
#   return rawData



# !!! filtering by vote percent requires the comment and participant count to be
# recalculated
# !!! test if bad comment accidentally removes post
# test if bad post removes all comments
# !!! counting OP as participant may not be valid since the post may be old
# !!! test if op out of date range correctly counted in participants
def topXReport(rawData, reportFile = None, rankVar = "Comments", minVotePct = 0, DiscuitURL = ""):
  if rankVar == "Comments":
    discRankVar = "TotalEngagement"
  elif rankVar == "Participants":
    discRankVar = rankVar
  else:
    print("rankVar needs to be Comments or Participants to force the use "
          "of TotalEngagement or Participants in the disc rankings.")
    raise BaseException

  # vote percent = 100 * upvotes / (upvotes + downvotes) and default to 100 if zero
  # which can only happen if submitter undoes their auto vote
  rawData = rawData.copy() # make a local copy instead of referencing original
  # if a post is "bad," it and all its comments should be hidden
  # if a comment is "bad," it's deleted but its post can stay if it is voted enough
  rawData["VotePct"] = (100 * rawData["Upvotes"] / (rawData["Upvotes"] + rawData["Downvotes"])).fillna(100)
  badPosts = rawData.query("(Type != 'Comment') & (VotePct < @minVotePct)")[["PublicId"]].drop_duplicates()
  rawData = (
    rawData.reset_index()
    .merge(badPosts, on = "PublicId", how = "left", indicator = True)
    .query("_merge == 'left_only'")
    .drop(columns = "_merge")
  )
  rawData = rawData.query("(Type != 'Comment') | (VotePct >= @minVotePct)").set_index("index")
  # grouping by post's publicId, comment count is total count minus 1 (the post)
  rawData["Comments"] = (
    rawData.groupby("PublicId")["Type"]
      .transform(lambda x: pandas.Series.count(x) - 1))
  # participants is unique users including OP, but need to filter for dates
  # create a fake name column to null out-of-date-range users
  rawData["FakeName"] = rawData["User"]
  rawData.loc[
    ~(
      (fromDate <= rawData["CreateDate"]) &
      ((rawData["CreateDate"] <= toDate) | (toDate == ""))),
    "FakeName"
  ] = None
  rawData["Participants"] = (
    rawData.groupby("PublicId")["FakeName"]
      .transform(
        lambda x: pandas.Series.nunique(x)))
  rawData.drop(columns = "FakeName", inplace = True)

  contentTypes = ["Texts", "Images", "Links", "Comments"]
  nonBot = rawData[~rawData["IsBot"].astype(bool) &
                   ~rawData["PartialBot"].astype(bool)]
  # comments in the dataframe should all be within the date range already
  sumPostComments = len(
    nonBot.query("(Type == 'Comment') & (@fromDate <= CreateDate) & "
                 "((CreateDate <= @toDate) | (@toDate == ''))"))
  numDiscs = len(nonBot['Disc'].unique())
  activeUsers = len(
    nonBot
    .query("(@fromDate <= CreateDate) & ((CreateDate <= @toDate) | (@toDate == ''))")['User']
    .unique())
  # includes posts that are not inside the date range, if a comment was made in range
  activePosts = len(nonBot['PublicId'].unique())

  print(f"\n# rankVar = {rankVar}, minVotePct = {minVotePct}\n", file = reportFile)

  print(f"\nDiscuit week in review: {fromDate}-{toDate}\n", file = reportFile)

  print(f"\n[Last week's report is here]({lastReportURL}).", file = reportFile)

  print("\nDiscuit API is [documented here](https://docs.discuit.org/getting-started). "
        "Source code of script generating the tables is "
        "[available here](https://github.com/reallytiredofclowns/discuitstats).", file = reportFile)
  registeredAccounts = requests.get(
    f"{baseURL}/api/_initial").json()["noUsers"]
  print(f"\n{activeUsers} users discussed {activePosts} posts in "
        f"{sumPostComments} comments over {numDiscs} total discs. "
        f"At the time of this report, there were {registeredAccounts} accounts.\n", file = reportFile)

  print("Felix30 has been [charting some of these numbers here](https://docs.google.com/spreadsheets/d/1H7zV_7YIZar9dwDHbutr0Dm9N6H-1mEXe0irIwSHsx0/edit#gid=1256137398). "
        "asyoucanseE_ [has alternative charting](https://sheet.zohopublic.eu/sheet/published/gr2z56fe0a19d6468429b9d88b3e60c81b23b).\n",
        file = reportFile)

  postTypes = rawData["Type"][rawData["Type"] != 'Comment'].unique()
  postTypes.sort()
  for postType in postTypes:
    subset = (rawData.query("Type == @postType")
      .drop(columns = ["Type", "PublicId"]).copy())
    if len(subset):
      # this really should be moved to the data capture section... or not? that would write escapes to CSV
      subset["User"] = subset["User"].str.replace("_", "\\_")
      subset["Rank"] = subset[rankVar].rank(method = "min", ascending = False)
      subset = subset.query("Rank <= @topX")
      subset = subset.sort_values("Rank")
      # if Title is all whitespace, print a fake string of &nbsp; so the
      # anchor isn't broken
      allBlank = ~subset["Title"].str.fullmatch(r"^.*[^\s].*$")
      subset.loc[allBlank, "Title"] = "&nbsp;" * 10
      subset["Title"] = (
        "[" + subset['Title'] + f"]({DiscuitURL}/" + subset['Disc'] +
        "/post/" + subset.index + ")")
      subset = subset[["Rank", "Disc", "Title", "User", rankVar]]
      print(f"## Top {topX} most engaging {postType}s:", file = reportFile)
      print(subset.to_markdown(index = False), file = reportFile)
      print("\n\n", file = reportFile)

  # # top comment, by votes, filtered
  # subset = rawData.query("(Type == 'Comment') & (VotePct >= @minVotePct)").copy()
  # subset["Rank"] = subset["Upvotes"].rank(method = "min", ascending = False)
  # subset = subset.query("Rank <= @topX")
  # subset = subset.sort_values("Rank")
  # # restrict comment link text to 100 chars
  # subset["CommentBody"] = subset["CommentBody"].str.replace("\n", " ")
  # subset.loc[
  #   subset["CommentBody"].str.len() > 100,
  #   "CommentBody"
  # ] = subset["CommentBody"].str.slice(0, 100) + "..."

  # subset["CommentBody"] = (
  #   "[" + subset["CommentBody"] + f"]({DiscuitURL}/" + subset['Disc'] +
  #   "/post/" + subset.index + ")")
  # subset = subset[["Rank", "Disc", "CommentBody", "User", "Upvotes"]]
  # print(f"## Top {topX} most upvoted comments:", file = reportFile)
  # print(subset.to_markdown(index = False), file = reportFile)
  # print("\n\n", file = reportFile)


  # disc activity
  subset = nonBot.copy()
  # don't count posts created out-of-date-range (could have been included
  # due to comments being in date range)... comments should already be in range
  deletes = subset[
    (
      (subset["CreateDate"] > toDate) |
      ((fromDate != "") & (subset["CreateDate"] < fromDate))
    )
  ].index
  subset = subset.drop(index = deletes)
  subset["Type"] = subset["Type"] + "s"

  # need to recalculate participants here at disc level, not post
  participants = (
    subset.groupby("Disc", as_index = False)["User"].nunique()
    .rename(columns = {"User": "Participants"}))
  subset = (subset.groupby(["Disc", "Type"], as_index = False)
    .size().pivot(columns = "Type", index = "Disc", values = "size"))
  subset = (
    subset.merge(participants, how = "left", on = "Disc")
    .reset_index().fillna(0))
  # if none of a post type/comment, need to create a zeroed column so it exists
  for contentType in contentTypes:
    if contentType not in subset:
      subset[contentType] = 0
  subset["TotalPosts"] = subset["Texts"] + subset["Images"] + subset["Links"]
  subset["TotalEngagement"] = subset["TotalPosts"] + subset["Comments"]
  subset["Rank"] = subset[discRankVar].rank(method = "min", ascending = False)
  subset = subset.query("Rank <= @topX")
  subset = subset.sort_values("Rank")
  subset = subset[["Rank", "Disc", "Texts", "Images", "Links", "TotalPosts", "Comments", discRankVar]]
  subset["Disc"] = "[" + subset["Disc"] + f"]({DiscuitURL}/" + subset["Disc"] + ")"
  print(f"## Top {topX} most engaging Discs:", file = reportFile)
  print(subset.to_markdown(index = False), file = reportFile)
  print("\n", file = reportFile)

  # user activity--remove Ghost and bot users from the active users table
  subset = rawData.query("(User != 'ghost') & ~IsBot & ~PartialBot").copy()
  deletes = subset[
    (subset["Type"] != "Comment") &
    (
      (subset["CreateDate"] > toDate) |
      ((fromDate != "") & (subset["CreateDate"] < fromDate))
    )
  ].index
  subset = subset.drop(index = deletes)
  subset["Type"] = subset["Type"] + "s"
  subset = (subset.groupby(["User", "Type"], as_index = False)
    .size()
    .pivot(columns = "Type", index = "User", values = "size")
    .reset_index()
    .fillna(0))
  # if none of a post type/comment, need to create a zeroed column so it exists
  for content in contentTypes:
    if content not in subset:
      subset[content] = 0
  subset["TotalPosts"] = subset["Texts"] + subset["Images"] + subset["Links"]
  subset["TotalEngagement"] = subset["TotalPosts"] + subset["Comments"]
  # users should always be ranked by total engagement after filtering
  subset["Rank"] = subset["TotalEngagement"].rank(method = "min", ascending = False)
  subset = subset.query("Rank <= @topX")
  subset = subset.sort_values("Rank")
  subset = subset[["Rank", "User", "Texts", "Images", "Links", "TotalPosts", "Comments", "TotalEngagement"]]
  subset["User"] = "[" + subset["User"] + f"]({DiscuitURL}/@" + subset["User"] + ")"
  print(f"## Top {topX} most engaged Discuiteers:", file = reportFile)
  print(subset.to_markdown(index = False), file = reportFile)

######################################################

rawData = generateTables(nextPage)
if exportCSV:
  rawData.drop(columns = ["Upvotes", "Downvotes", "CommentBody"]).to_csv(exportCSV, index_label = "index")
#rawData = finishData(rawData)
if reportFileName:
  with open(reportFileName, "w") as reportFile:
    topXReport(rawData, reportFile)
    # topXReport(rawData, reportFile, rankVar = "Comments", minVotePct = 50, DiscuitURL = baseURL)

    # topXReport(rawData, reportFile, rankVar = "Participants", minVotePct = 0, DiscuitURL = baseURL)
    # topXReport(rawData, reportFile, rankVar = "Participants", minVotePct = 50, DiscuitURL = baseURL)

else:
  topXReport(rawData)

