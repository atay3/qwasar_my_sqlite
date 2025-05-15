require_relative "my_sqlite_request"

request = MySqliteRequest.new
#  ✅test run() - check for empty
# p request.run

#   emojis for testing status
#⚠️
#❌
#✅

#   test from()
#   invalid file
request = request.from("noob")
#   valid file
# request = request.from("student")
p request