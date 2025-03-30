require 'csv'

#   research
#   https://www.rubyguides.com/2018/10/parse-csv-ruby/

#   note to self / TODO
#       project requirements - add row id
#       parse column table - to lowercase because its not case-sensitive

class MySqliteRequest
    #   It will have a similar behavior than a request on the real sqlite
    #   All methods, except run, will return an instance of my_sqlite_request
    #   You will build the request by progressive call and execute the request by calling run
    #   Each row must have an ID.
    #   We will do only 1 join and can do one or multiple where(s) per request.
    # @my_sqlite_request = []
    
    @table_data = nil
    @selected_columns = nil
    @where_result = nil
    @insert_values_data = nil
    @query_result = []

    # @my_sqlite_request = []
    # @request_errors = []
    # @from_table = {}

    attr_accessor :table_data, :from_table, :insert_values_data, :my_sqlite_request
    #, :from_table_one, :from_table_two
    attr_reader :request_errors
#     Constructor It will be prototyped:
    def initialize()
        @my_sqlite_request = []
        @request_errors = []
        @from_table = {}
    end

    #   checks for errors in the request
    def check_for_error
        #   any error(s) found
        @request_errors.nil?
    end

    def get_my_sqlite_request
        @my_sqlite_request
    end

    #   appends a valid request to the request chain for my_sqlite
    def add_my_sqlite_request(statement)
        #   for debugging
        puts "current request\n#{@my_sqlite_request}"
        puts "adding to request - #{statement}"
        #   end debugging
        @my_sqlite_request.append(statement)
    end

    def check_duplicate_statements(statement, request_query)
        #   check if prior requests
        if get_my_sqlite_request.length > 0
            is_duplicate = request_query.any? {|query| @my_sqlite_request.include?(query)}
            if is_duplicate
                # add_error("only 1 #{statement} per request [SELECT | UPDATE | INSERT | DELETE]")
                add_error("only 1 #{statement} per request [#{request_query.join(" | ")}]")
                return true
            end
        end
        return false
    end

    #   project rules for JOIN and WHERE
    #   1 join -> many wheres
    #   0 join -> 1 where
    def check_duplicate_where_statement(statement)
        join_count = @my_sqlite_request.count("JOIN")
        if join_count == 0
            where_count = @my_sqlite_request.count("WHERE")
            if where_count == 1
                add_error("only 1 #{statement} per 0 JOIN or many per 1 JOIN")
                return true
            end
        end
        return false
    end

    def check_sqlite_statement(statement)
        #   check if same statement already requested
        case statement
        when "SELECT", "UPDATE", "INSERT", "DELETE"
            return -2 if check_duplicate_statements(statement, ["SELECT", "UPDATE", "INSERT", "DELETE"])
            return 0
        when "FROM"
            return -3 if check_duplicate_statements(statement, ["FROM"])
            return 1
        when "JOIN"
            return -4 if check_duplicate_statements(statement, ["JOIN"])
            return 2
#       >>>>>>>>>>>>>>> this where is going to fail, please refactor <<<<<<<<<<<
        when "WHERE"
            #   check if join exists
            return -5 if check_duplicate_where_statement(statement)
            return 3
        when "INSERT", "VALUE"
            return -6 if check_duplicate_statements(statement, ["INSERT", "VALUE"])
            return 4
        end
        #   invalid sqlite statement
        add_error("invalid sqlite statement")
        return -1
    end
    
=begin  old implementation, no longer used
    def get_current_table
        # return -1 if @current_table.nil?
        @current_table
    end

    def get_all_table_data
        @table_data
    end
=end

=begin  old implementation, not that modular
    def get_table_headers(file_name)
        # @table_data[0]
        CSV.read(file_name, converters: :all)[0]
    end
=end
    def get_table_headers(table_data)
        return table_data[0]
    end
    #   old idea for headers - changed to get current table headers
=begin
    def get_table_one_headers
        @from_table_one[0]
    end

    def get_table_two_headers
        @from_table_two[0]
    end
=end
    def read_csv_file(file_name)
        return CSV.read(file_name, converters: :all)
    end
    
    def add_error(error_msg)
        @request_errors.append(error_msg)
    end
#   1
# From Implement a from method which must be present on each request. 
# From will take a parameter and it will be the name of the table
# (technically a table_name is also a filename (.csv))
# It will be prototyped:

    def check_filename(file_name)
    # def match_filename(table_name)
        # if test_file != 'nba_play_data.csv' do
        #     return 1
        # elsif test_file != 'nba_players.csv' do
        #     return 2
        # else
        #     return 0

        #   more modular implementation with file exists check
        ##  TODO check if file is actually a csv file
        return 0 if File.exist?(file_name)
        add_error("Error: no such table: #{file_name.split(".")[0]}")
        return -1
    end

    #   TODO check for specific .csv files?
    #       nba_players.csv nba_player_data.csv

    def get_table_data(table_name)
        table_hash = {
            name: table_name,
            data: read_csv_file(table_name),
        }
        table_hash[:headers] = get_table_headers(table_hash[:data])
        return table_hash
    end
    #   for checking csv
    def from(table_name)
        return false if check_for_error
        #   check if file is valid
        return -1 if !check_filename(table_name)
        #   check if FROM already requested
        statement_result = check_sqlite_statement("FROM")
        if statement_result >= 0
            #   update ongoing request
            add_my_sqlite_request("FROM")
            #   read and save csv contents
=begin  old implementation - separated into modular function
            @from_table = {
                name: table_name,
                data: read_csv_file(table_name),
                # headers: get_table_headers(table_name)
            }
            @from_table[:headers] = get_table_headers(@from_table[:data])
=end
            @from_table = get_table_data(table_name)
            # p @from_table
            return 0
        end
        return -2
    end
#   2
#   Select Implement a where method which will take one argument 
#   a string OR an array of string. It will continue to build the request. 
#   During the run() you will collect on the result only the columns sent as parameters to select :-).
# It will be prototyped:

=begin  old implementation - not modular
    def check_columns(column_name, file_name)
        get_table_headers(file_name).include?(column_name)
    end
=end

    def check_columns(column_name, table_headers)
        table_headers.include?(column_name)
    end
# def select(column_name)
# OR
# def select([column_name_a, column_name_b])
    def select(column_name)
        return false if check_for_error
        parsed_columns = nil
        # puts "col name is #{column_name}"
        case
        when column_name.class == Array
            puts "select - type array"
            parsed_columns = column_name if column_name.all? {|x| check_columns(x)}
        when (column_name.is_a? String)
            puts "select - type string"
            # TODO "add check if string is * aka wildcard?"
            parsed_columns = [column_name] if check_columns(column_name)
        else
            puts "select - invalid [#{column_name.class}]"
        end
        return false if parsed_columns == nil
        @selected_columns = parsed_columns.map {|x| get_table_headers.find_index(x)}
        puts "selected cols %#{@selected_columns}"
    end
#   3
# Where Implement a where method which will take 2 arguments: column_name and value. It will continue to build the request. During the run() you will filter the result which match the value.
# It will be prototyped:

    def where(column_name, criteria)
        return false if check_for_error
        if check_columns(column_name)
            result = []
            where_column = get_table_headers.find_index(column_name)
            #   add each row of table data
            @table_data[1..-1].each do |data|
                tmp = []
                if data[where_column] == criteria
                    #   add each column from select query
                    @selected_columns.each do |index|
                        tmp.append(data[index])
                    end
                end
                result.append(tmp) if !tmp.empty?
            end
            #   no results from where query
            if result.empty?
                puts "where fail - no query results"
                return 0
            end
            @where_result = result
            p @where_result
            return 1
        end
        puts "where fail - invalid column(s)"
        return -1
    end
#   4
# Join Implement a join method which will load another filename_db and will join both database on a on column.
# It will be prototyped:

=begin      old implementation - no longer used
    def join_table_data(tmp_data)
        updated_table = [get_table_headers()]
        tmp_data.each do |key,val|
            tmp = [key, val]
            # tmp.flatten
            updated_table.append(tmp.flatten)
        end
        p updated_table
    end
=end
    def join_combine_tables(column_on_db_a, table_b_hash, column_on_db_b)
        #   create combined data
        table_combined = []
        # table_b_data = read_csv_file(filename_db_b)
        a_index = @from_table[:headers].find_index(column_on_db_a)
        # b_headers = get_table_headers(table_b_data)
        b_index = table_b_hash[:headers].find_index(column_on_db_b)
        #   add headers of both tables
        table_combined.append(@from_table[:headers] + table_b_hash[:headers])
        #   add rows from both if respective column values are equal
        @from_table[:data][1..-1].each do |a_row|
            table_b_hash[:data][1..-1].each do |b_row|
                if a_row[a_index] == b_row[b_index]
                    table_combined.append(a_row + b_row)
                end
            end
        end
        return table_combined
    end

    def join(column_on_db_a, filename_db_b, column_on_db_b)
        return false if check_for_error
        #   check for previous join
        if check_sqlite_statement("JOIN") >= 0
            #   check column for database a if exist
            if check_columns(column_on_db_a, @from_table[:headers]) == false
                add_error("Error: no such column #{column_on_db_a}")
                return -2
            end
=begin      from_table already stores this
            table_a_data = {
                name: @from_table[:name],
                data: @from_table[:data],
                headers: get_table_headers(@from_table[:name]),
            }
            puts "table a data #{table_a_data}"
=end
            # p from_table
            #   check filename database b if exists
            if check_filename(filename_db_b) == 0
                table_b_hash = get_table_data(filename_db_b)
                #   check column for database b if exists
                if check_columns(column_on_db_b, table_b_hash[:headers]) == false
                    add_error("Error: no such column #{column_on_db_b}")
                    return -3
                end
=begin      #   separated into a helper function
                table_b_data = read_csv_file(filename_db_b)
                #   create combined data
                table_combined = []
                #   add headers of both
                a_index = @from_table[:headers].find_index(column_on_db_a)
                b_headers = get_table_headers(filename_db_b)
                b_index = b_headers.find_index(column_on_db_b)
                table_combined.append(@from_table[:headers] + b_headers)
                #   add rows from both if column values are equal
                @from_table[:data][1..-1].each do |a_row|
                    table_b_data[1..-1].each do |b_row|
                        if a_row[a_index] == b_row[b_index]
                            table_combined.append(a_row + b_row)
                        end
                    end
                end
=end
                # @table_data = table_combined
                @table_data = join_combine_tables(column_on_db_a, table_b_hash, column_on_db_b)
                # p @table_data
                add_my_sqlite_request("JOIN")
                return 0
            end
        end
        return -1
=begin old implementation
        if from(filename_db_b) > 0
            #   check if column b exists in filename b
            puts "here"
            return -2 if check_columns(column_on_db_b, filename_db_b) == false
            #   if col_a data matches col_b data
            puts "there"
            tmp = {}
            column_a_index = table_a_headers.find_index(column_on_db_a)
            column_b_index = get_table_headers.find_index(column_on_db_b)
            #   table a stuff
            # p "col a indx #{column_a_index} b #{column_b_index}"
            # p table_a_data
            table_a_data[1..5].each do |a_data|
                # p "a_data #{a_data}"
                # p "-------------1"
                a_hash_key = a_data[column_a_index]
                # p "a hash key #{a_hash_key}"
                # p "-------------2"
                tmp[a_hash_key] = a_data.reject {|x| x == a_data[column_a_index]}
                # p "tmp[a_hash_key] #{tmp[a_hash_key]}"
                # p "-------------3"
            end
            # puts "nono\n#{tmp}"
            #   table b stuff
            @table_data[1..5].each do |b_data|
                # p b_data
                b_hash_key = b_data[column_b_index]
                # p b_hash_key
                if tmp.has_key?(b_hash_key)
                # b_hash_value_list = tmp[b_hash_key]
                    tmp[b_hash_key].append(b_data.reject {|y| y == b_data[column_b_index]})
                end
            end
            puts "join\n"
            puts tmp
            #   combine data in tmp to table data format
            join_table_data(tmp)
        end
        add_error("Error: no such column #{column_on_db_b}")
        return -3
=end
    end
#   5
# Order Implement an order method which will received two parameters, order (:asc or :desc) and column_name. It will sort depending on the order base on the column_name.
# It will be prototyped:

    def check_order(order)
        puts "order is #{order}"
        case order
        when :asc
            puts "asc"
            return 1
        when :desc
            puts "desc"
            return -1
        else
            return 0
        end
    end
    
    def order_sort(current_table, column_name, valid_order, table_headers)
        # p "valid order #{valid_order}"
        #   sort table data by column depending on order
        column_index = table_headers.find_index(column_name)
        # p "sorting by col #{column_index}"
        result = current_table[1..-1].sort_by { |row| row[column_index] * valid_order }
        # p "result\n#{result}"
        #   combine table data - headers + sorted result
        tmp_table = [table_headers]
        tmp_table.append(result)
        return tmp_table
    end

    def order(order, column_name)
        #   check column name if exists
        if @table_data
            current_table = @table_data
            table_headers = @table_data[0]
        elsif @from_table
            current_table = @from_table
            table_headers = @from_table[:headers]
        end
        # p current_table
        #   check for valid column
#   TODO >>>>>>>>>>>>>>>>        add valid column check table.column notation <<<<<<<<
        if check_columns(column_name, table_headers) == false
            add_error("Error: no such column #{column_name}")
            return -1
        end
#   TODO >>>>>>>>>>>>>>         add to_lower check? <<<<<<<<<<<
        #   check if duplicate column
        if table_headers.count(column_name) > 1
            add_error("Error: ambiguous column name: #{column_name}")
            return -2
        end
        #   check if valid order
        valid_order = check_order(order)
        if valid_order != 0
            @table_data = order_sort(current_table, column_name, valid_order, table_headers)
            add_my_sqlite_request("ORDER")
            return 0
        end
        add_error("Invalid Order - [ ASC | DESC ] only")
        return -3
    end

    # def insert_into_values
    # end
#   6
# Insert Implement a method to insert which will receive a table name (filename). It will continue to build the request.
    def insert(table_name)
        #   check table if exists
        if check_filename(table_name) == 0
            #   check current sqlite request
            if check_sqlite_statement("INSERT") == 0
                #   read file - csv to list?
                table_data = read_csv_file(table_name)
                #   update @insert_values_data
                @insert_values_data = {insert: get_table_data(table_name)}
                #   add sqlite request
                add_my_sqlite_request("INSERT")
                return 0
            end
            add_error("duplicate statement - only 1 of each [INSERT | VALUES]")
            return -2
        end
        return -1
    end

    def check_values(value_data)
        p "value data - #{value_data}"
        p "insert values data - #{@insert_values_data}"
        #   check column count
        p "num of cols - check if match"
        #   check data type per column
        p "col data - check if valid"
        return 0
    end
#   7
# Values Implement a method to values which will receive data. (a hash of data on format (key => value)). It will continue to build the request. During the run() you do the insert.
    def values(data)
    #     #   check current sqlite request
        if check_sqlite_statement("VALUE") == 4
            if check_values(data) == 0
                #   check number of elements
                #   check element types
                #   add sqlite request
            add_my_sqlite_request("VALUE")
    #     #   update @insert_values_data
                return 0
            end
        end
        add_error("invalid values statement - [not enough values current/required]")
        return -1
    end
#   8
# Update Implement a method to update which will receive a table name (filename). It will continue to build the request. An update request might be associated with a where request.
def update(table_name)
    table = @tables[table_name]
    return unless table # Return early if table DNE

    table.each do |row|
        if @conditions.nil? || @conditions.all? { |col, val| row[col] == val }
            @updates.each { |col, val| row[col] = val if row.key?(col) }
        end
    end
end

#   9
# Set Implement a method to update which will receive data (a hash of data on format (key => value)). It will perform the update of attributes on all matching row. An update request might be associated with a where request.
# def set(data)

#   10
# Delete Implement a delete method. It set the request to delete on all matching row. It will continue to build the request. An delete request might be associated with a where request.
# def delete

#   11
# Run Implement a run method and it will execute the request.
end