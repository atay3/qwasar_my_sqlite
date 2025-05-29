require_relative "my_sqlite_request"

request = MySqliteRequest.new
#  ✅test run() - check for empty
# p request.run

#   emojis for testing status
#⚠️
#❌
#✅

#   test from()
request = request.from("student.csv")
# request = request.select("id")
table_all = request.get_table_data
headers = table_all[:headers]
table_data = table_all[:data][1..-1]

def p_headers(headers)
    puts "headers\n#{headers}\n"
end

def p_table_data(table_data)
    puts "table"
    table_data.each {|x| puts x.inspect}
end

def p_errors(errors)
    puts "errors - #{errors}"
end

def p_queue(queue)
    puts "queue - #{queue}"
end

def p_q_result(result)
    puts "result - #{result}"
end

p_headers(headers)
p_table_data(table_data)
p_errors(request.get_request_errors)
p_queue(request.get_request_queue)
request = request.select("")
# p_q_result(result.queue_result)
# request.run

#CHECKED
#   -from()
#   -select() - fully tested
#       -string - 1 column name
#           -empty - PASS   i.e. [""] in ["a", "b"]
#           -exists - PASS  i.e. ["a"] in ["a", "b"]
#           -does not exist - PASS i.e. ["c"] in ["a", "b"]
#           -"*" all edgecase - PASS
#       -array
#           -empty - PASS i.e. [] in [a,b,c]
#           -every in array in header - PASS i.e. [a,b,c] in [a,b,c,d]
#           -some of array in header - PASS i.e. [a,b,e] in [a,b,c,d]

#TODO
#   create run_from()
#   check if select() - array - values need to be parsed or not