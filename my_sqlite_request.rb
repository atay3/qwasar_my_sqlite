require 'csv'

#   research
#   https://www.rubyguides.com/2018/10/parse-csv-ruby/

#   note to self / TODO
#       project requirements - add row id
#       parse column table - to lowercase because its not case-sensitive

class MySqliteRequest
    #   It will have a similar behavior than a request on the real sqlite
    #   All methods, except run, will return an instance of request_queue
    #   You will build the request by progressive call and execute the request by calling run
    #   Each row must have an ID.
    #   We will do only 1 join and can do one or multiple where(s) per request.

    attr_accessor :request_queue,:table_data, :insert_values_data
    attr_reader :request_errors
#   Constructor It will be prototyped:
    def initialize()
        #   request
        @request_queue = []
        @request_errors = []
        #   table data
        @table_data = nil
        #   selected columns
        @selected_columns = nil
        #   where_result
        @where_result = nil
        #   values for insert?
        @insert_values_data = nil
        #   final result for queue
        @request_result = []
    end

    def check_for_error
        @request_errors.empty?
    end

    def get_request_queue
        @request_queue
    end

    def add_request_queue(statement)
        p "current #{@request_queue}"
        @request_queue.append(statement)
    end

    def check_duplicate_statements(statement, request_query)
        #   check if prior requests
        if get_request_queue.length > 0
            is_duplicate = request_query.any? {|query| @request_queue.include?(query)}
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
        join_count = @request_queue.count("JOIN")
        if join_count == 0
            where_count = @request_queue.count("WHERE")
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
        # end
        when "SET"
            return -7 if check_duplicate_statements(statement, ["SET"])
            return 5
        when "DELETE"
            return -8 if check_duplicate_statements(statement, ["DELETE"])
            return 6
        #   invalid sqlite statement
        end
        add_error("invalid sqlite statement")
        return -1
    end

    def get_table_headers(table_data)
        return table_data[0]
    end
    # def get_table_headers()
    #     return @table_data[0]
    # end
    #   old idea for headers - changed to get current table headers

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
        #   more modular implementation with file exists check
        ##  TODO check if file is actually a csv file
        puts -3
        return 0 if File.exist?(file_name)
        puts -4
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
            add_request_queue("FROM")
            #   read and save csv contents
            @from_table = get_table_data(table_name)
            # p @from_table
            # return 0
            return self
        end
        return -2
        # self
    end
#   2
#   Select Implement a where method which will take one argument 
#   a string OR an array of string. It will continue to build the request. 
#   During the run() you will collect on the result only the columns sent as parameters to select :-).
# It will be prototyped:

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
        return self if check_for_error

        # header = @from_table[:headers]
        # where_column = header.find_index(column_name)
        where_column = get_table_headers(column_name)

        # if check_columns(column_name, header)
        if check_columns(column_name, where_column)
            result = []
            # where_column = get_table_headers.find_index(column_name)
            #   add each row of table data

            @table_data[1..-1].each do |data|
                tmp = []
                if data[where_column] == criteria
                    #   add each column from select query
                    @selected_columns.each do |index|
                        tmp.append(data[index])
                    end
                end
                if row[where_column] == criteria
                    @selected_columns.each do |index|
                      tmp << row[index]
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
            # p from_table
            #   check filename database b if exists
            if check_filename(filename_db_b) == 0
                table_b_hash = get_table_data(filename_db_b)
                #   check column for database b if exists
                if check_columns(column_on_db_b, table_b_hash[:headers]) == false
                    add_error("Error: no such column #{column_on_db_b}")
                    return -3
                end
                # @table_data = table_combined
                @table_data = join_combine_tables(column_on_db_a, table_b_hash, column_on_db_b)
                # p @table_data
                add_request_queue("JOIN")
                return 0
            end
        end
        return -1
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
            add_request_queue("ORDER")
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
                add_request_queue("INSERT")
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
            add_request_queue("VALUE")
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
        puts -5
        if !check_filename(table_name)
            puts -1
            return -1
        end

        if check_sqlite_statement("UPDATE") == -2
            puts -2
            return -2
        end

        add_request_queue("UPDATE #{table_name}")
        # @table_name = table_name
        @update_table = get_table_data(table_name)
        puts "Updating table..."
        self
        # puts "upend\n"
    end

#   9
# Set Implement a method to update which will receive data (a hash of data on format (key => value)). It will perform the update of attributes on all matching row. An update request might be associated with a where request.
    def set(data)
        if check_sqlite_statement("SET") == -7
            return -7
        end
        add_request_queue("SET #{data}")
        @update_data = data
        puts "setting data..."
        self
    end

#   10
# Delete Implement a delete method. It set the request to delete on all matching row. It will continue to build the request. An delete request might be associated with a where request.
    def delete
        if check_sqlite_statement("DELETE") == -8
            return -8
        end
        add_request_queue("DELETE")
        self # Return self for chaining
    end

#   11
# Run Implement a run method and it will execute the request.
    def run()
        execute_requests()
    end

    def execute_requests()
        #   check if request is empty
        return "request queue - empty" if check_for_error()
        #   execute request(s)
        @queue_result = @request_queue.map {
            #   iterate through the queue and execute each section
            
        }
    end
    
    def run_update()
        table_name = @from_table[:name]
        headers = @table_data.first.headers

        updated_rows = []

        @table_data.each do |row|
            if @where_result.nil? || row[@where_result[:column]] == @where_result[:value]
              @update_data.each do |col, val|
                row[col] = val
              end
            end
            updated_rows << row
        end

        CSV.open(table_name, "w", write_headers: true, headers: headers) do |csv|
            updated_rows.each do |row|
                csv << row
            end
        end
    end
end
