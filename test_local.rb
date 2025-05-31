require_relative "my_sqlite_request"
require 'pp'

request = MySqliteRequest.new
#  ✅test run() - check for empty
# p request.run

#   emojis for testing status
#⚠️
#❌
#✅

#   test setup
# table_all = nil
table_data = nil
headers = nil
#   test from()
request = request.from("student.csv")
# request = request.select("id")
# table_all = request.get_table_data
headers = request.get_table_headers
table_data = request.get_table_data

def p_table_info(table_info)
    if table_info
        puts "table\n"
        table_info.each {|row| pp row}
    else
        puts "empty table info"
    end
end

def p_headers(headers)
    if headers
        puts "headers\n#{headers}\n"
    else
        puts "empty headers\n"
    end
end

def p_table_data(table_data)
    if table_data
        puts "table data"
        # table_data.each {|x| puts x.inspect}
        table_data[:data].each {|row| pp row}
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

def p_r_queue(queue)
    puts "result queue - #{queue}"
end

def p_r_result(result)
    if result
        puts "full request result\n#{result}"
    else
        puts "request sult is EMTPY"
    end
end

#   test part2
request = request.select(["id", "age"])
p_headers(headers)
p_table_data(table_data)
if request
    p_errors(request.get_request_errors)
else
    puts "empty request"
end
p_r_queue(request.get_request_queue)
p_r_result(request.get_request_result)
request.run

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