require_relative '../my_sqlite_request'
require_relative '../my_sqlite_cli'

request = MySqliteRequest.new
request = request.from("test.csv")
p request