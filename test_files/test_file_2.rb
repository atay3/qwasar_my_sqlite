require './my_redo'
require './my_sqlite_cli'

request = MySqliteRequest.new
# request.test
=begin old tests
# puts request.table_data                                     #   passes
# puts request.from('nba_players.csv')                        #   passes
# puts "t1 #{request.from_table_one}"                          #   FAIL
# puts request.get_table_one_headers                          #   passes
# puts request.get_table_two_headers                          #   passes

# puts request.from('nba_player_data.csv')                    #   passes
# puts "curr table [#{request.get_current_table}]"            #   passes
# puts "from table 2 [#{request.from_table_two}]"
# puts "get t1 [#{request.get_table_one}]"
# puts request.get_table_one
=end

# puts request.from('nba_players.csv')  ----
# puts request.from('nba_player_data.csv')                    #   passes
# puts request.get_table_headers                              #   passes
# puts request.select(['name', 'weight']) 
# puts request.select(['Player', 'weight'])                        #   passes
# request.where('birth_state', 'Indiana')        
# request.join('Player', 'nba_player_data.csv', 'name') ----
# p request.get_table_headers

request.from('test.csv')
p request.get_my_sqlite_request

p request.from('class.csv')
# p "errors #{[request.request_errors]}"
# p request.get_my_sqlite_request

# p request.get_table_headers(request.from_table[:name])
# ----------------
    # request.join('ID', 'student.csv', 'ID')
#       ^^^^^^^^^^^^^
    # p "insert result1 #{request.insert('student.csv')}"
# p "insert result2 #{request.insert('student.csv')}"
    # p request.insert_values_data
# request.order(:asc, "TYPE")
# request.order(:desc, "TYPE")
#   -------------------------
# p "test_file table data\n #{request.table_data}"
    # p "errors #{request.request_errors}"
    # p "my_sqlite request #{request.my_sqlite_request}"
    # p "tabledata #{request.table_data}"

# TODO values

#   table query order? maybe use a stack to keep the order?
#   A       B       QUERY     manipulate data
#   FROM -> JOIN -> WHERE -> SELECT|INSERT|UPDATE|DELETE
#                     C           D
#       $$optional$$
#   SELECT * or whatever columns
#   INSERT INTO [table name] VALUES (vals, ...)
#   UPDATE [table name] SET [column = col value] $$WHERE$$
#   DELETE FROM [table name] $$WHERE$$
#
#   note for building query (for future implementation)
#   make sure A + D exists, B and C is optional
#
#   SELECT - filter data
#   INSERT - creating new data for each column
#   UPDATE - changing data
#   DELETE - removing row?