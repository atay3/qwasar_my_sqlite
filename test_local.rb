require_relative "my_sqlite_request"

request = MySqliteRequest.new
#  ✅test run() - check for empty
# p request.run

#   emojis for testing status
#⚠️
#❌
#✅

table_all = nil
table_data = nil
headers = nil
#   test from()
# request = request.from("student.csv")
# # request = request.select("id")
# table_all = request.get_table_data
# headers = table_all[:headers]
# table_data = table_all[:data][1..-1]

def p_headers(headers)
    if headers
        puts "headers\n#{headers}\n"
    else
        puts "empty headers\n"
    end
end

def p_table_data(table_data)
    if table_data
        puts "table"
        table_data.each {|x| puts x.inspect}
    else
        puts "empty table"
    end
end

def p_errors(errors)
    if errors
        puts "errors - #{errors}"
    else
        puts "empty errs"
    end
end

def p_queue(queue)
    puts "queue - #{queue}"
end

def p_q_result(result)
    puts "result - #{result}"
end

request = request.select(["id", "name"])
p_headers(headers)
p_table_data(table_data)
if request
    p_errors(request.get_request_errors)
    p_queue(request.get_request_queue)
    p_queue(request.get_request_queue)
    request.run_select
else
    puts "empty request"
end
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