require 'csv'

#   research
#   https://www.rubyguides.com/2018/10/parse-csv-ruby/

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
        puts "DEBUG: Checking errors. Current errors: #{@request_errors.inspect}"
        !@request_errors.empty?
    end

    def get_request_result
        @request_result
    end

    def get_request_queue
        @request_queue
    end

    def add_request_queue(statement)
        @request_queue.append(statement)
    end

    def check_duplicate_statements(statement, request_query)
        #   check if prior requests exist
        if get_request_queue.length > 0
            is_duplicate = request_query.any? {|query| @request_queue.include?(query)}
            if is_duplicate
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
        when "WHERE"
            return -5 if check_duplicate_where_statement(statement)
            return 3
        when "SET"
            return -7 if check_duplicate_statements(statement, ["SET"])
            return 5
        when "DELETE"
            return -8 if check_duplicate_statements(statement, ["DELETE"])
            return 6
        when "VALUE"
            return -6 if check_duplicate_statements(statement, ["VALUE"])
            return 4
        end
        add_error("invalid sqlite statement")
        return -1
    end

    def get_table_headers
        @table_data[:headers] if @table_data
    end

    def read_csv_file(file_name)
        return CSV.read(file_name, converters: :all)
    end
    
    def add_error(error_msg)
        @request_errors.append(error_msg)
    end

    def normalize_table_name(name)
        name.end_with?(".csv") ? name : "#{name}.csv"
    end

    #   returns request_errors FOR DEBUGGING
    def get_request_errors
        @request_errors
    end

    def check_file_name_length(file_name)
        #   minimum name length - 5 i.e. "a.csv"
        if file_name.length < 5
            add_error("invalid file_name length\n")
            return -1
        #   ending in .csv
        elsif file_name[-4..-1] != ".csv"
            add_error("invalid file extension\n")
            return -2
        #   character check - "a..z_"
        elsif !/[a-z_]/.match(file_name)
            add_error("invalid filename characters")
            return -3
        end
        p "file_name pass\n"
        return 0
    end

    def check_file_exist(file_name)
        if File.exist?(file_name)
            p "File exist msg"
            return 0
        end
        add_error("File does not exist.")
        return -1
    end

    def check_filename(file_name)
        #   more modular implementation with file exists check
        ##  TODO check if file is actually a csv file
        puts "check_filename [#{file_name}]\n"

        #   check file_name length
        if !check_file_name_length(file_name)
            return -1
        end
        if check_file_exist(file_name)
            return 0
        end
        return -2
    end

    def set_table_data(table_name)
        data_from_csv = read_csv_file(table_name) 
        table_hash = {
            name: table_name,
            headers: data_from_csv[0],
            data: data_from_csv[1..-1],
        }
        return table_hash
    end

    #   returns all info of table
    def get_table_info
        @table_data
    end

    #   returns only data of table
    def get_table_data
        @table_data
    end

    #   for checking csv
    def from(table_name)
        #   check for errors
        return false if check_for_error

        table_name = normalize_table_name(table_name)

        #   check if file is valid
        return -1 if !check_filename(table_name)
        return false if check_for_error

        #   check if FROM already requested
        if check_sqlite_statement("FROM") >= 0
            #   update ongoing request
            add_request_queue("FROM")
            #   read and save csv contents
            @from_table = set_table_data(table_name)
            # @table_data = set_table_data(table_name)
            @table_data = {
                name: table_name,
                headers: @from_table[:headers],
                data: @from_table[:data]
            }
            puts "Table data after load: #{@table_data.inspect}"
            return self
        end

        return self
    end

    def check_columns(column_name, table_headers)
        table_headers.include?(column_name)
    end

    def select(column_name)
        return false if check_for_error | !@table_data
        
        parsed_columns = nil
        headers = get_table_headers
        
        case column_name
        # Multiple columns
        when Array
            parsed_columns = column_name.map { |col| headers.index(col) }.compact
        when String
            # Select all columns
            if column_name == "*"
                parsed_columns = (0...headers.length).to_a
            else # Single columns
                parsed_columns = [headers.index(column_name)]
            end
        end

        if !parsed_columns.nil? && !parsed_columns.empty?
            @selected_columns = parsed_columns
            add_request_queue("SELECT")
        else
            add_error("select - invalid column(s): #{column_name}")
        end
        self
    end

    def where(column_name, criteria)
        return self if check_for_error

        if check_columns(column_name, get_table_headers)
            @where_result = {
                column: column_name,
                value: criteria
            }
            add_request_queue("WHERE")
            return self
        end
        add_error("Invalid column in WHERE clause")
        self
    end

    def join_combine_tables(column_on_db_a, table_b_hash, column_on_db_b)
        # Get column indices
        a_index = @table_data[:headers].index(column_on_db_a)
        b_index = table_b_hash[:headers].index(column_on_db_b)
        
        # Combine headers
        combined_headers = @table_data[:headers] + table_b_hash[:headers]
        
        # Perform join
        joined_data = []
        @table_data[:data].each do |a_row|
            table_b_hash[:data].each do |b_row|
                if a_row[a_index] == b_row[b_index]
                    joined_data << a_row + b_row
                end
            end
        end
        
        # Return combined structure
        {
            name: @table_data[:name],
            headers: combined_headers,
            data: joined_data
        }
    end

    def join(column_on_db_a, filename_db_b, column_on_db_b)
        return false if check_for_error

        #   Check for previous join
        if check_sqlite_statement("JOIN") >= 0
            #   Check column for database a if exists
            unless @table_data && check_columns(column_on_db_a, @table_data[:headers])
                add_error("Error: no such column #{column_on_db_a}")
                return -2
            end

            filename_db_b = normalize_table_name(filename_db_b)

            #   Check filename database b if exists
            if check_filename(filename_db_b) == 0
                table_b_hash = set_table_data(filename_db_b)

                #   Check column for database b if exists
                if check_columns(column_on_db_b, table_b_hash[:headers]) == false
                    add_error("Error: no such column #{column_on_db_b}")
                    return -3
                end
                @table_data = join_combine_tables(column_on_db_a, table_b_hash, column_on_db_b)
                
                # Store join data
                @join_data = {
                    table_b: filename_db_b,
                    column_a: column_on_db_a,
                    column_b: column_on_db_b
                }

                add_request_queue("JOIN")
                return self
            end
        end
        return -1
    end

    def check_order(order)
        case order
        when :asc
            return 1
        when :desc
            return -1
        else
            return 0
        end
    end
    
    def order_sort(current_table, column_name, valid_order, table_headers)
        column_index = table_headers.find_index(column_name)
        rows = current_table[:data]
        
        column_index = table_headers.find_index(column_name)
        rows = current_table[:data]
        
        sorted_rows = rows.sort_by do |row|
            value = row[column_index]
            # For descending order, invert numeric values
            if value.is_a?(Numeric)
                valid_order == :desc ? -value : value
            else
                valid_order == :desc ? value.to_s.reverse : value.to_s
            end
        end

        # Return new table structure with sorted data
        {
            name: current_table[:name],
            headers: table_headers,
            data: sorted_rows
        }
    end

    def order(order, column_name)
        # Validate order direction first
        unless [:asc, :desc].include?(order)
            add_error("Invalid Order - [ ASC | DESC ] only")
            return -1
        end

        # Get table data
        current_table = @table_data || @from_table
        unless current_table
            add_error("No table data available")
            return -2
        end

        table_headers = current_table[:headers]
        
        # Check if column exists
        unless table_headers.include?(column_name)
            add_error("Error: no such column #{column_name}")
            return -3
        end
        puts "Before sort: #{@table_data[:data].inspect}"
        # Perform the sort
        @table_data = order_sort(current_table, column_name, order, table_headers)
        puts "After sort: #{@table_data[:data].inspect}"
        add_request_queue("ORDER")
        return 0
    end

    def insert(table_name)
        table_name = normalize_table_name(table_name)
        #   check table if exists
        if check_filename(table_name) == 0
            if check_sqlite_statement("INSERT") == 0
                @table_data = set_table_data(table_name)
                @insert_values_data = {insert: table_name}

                # add sqlite request
                add_request_queue("INSERT")
                return 0
            else
                add_error("duplicate INSERT statement")
                return -2
            end
        end
        return -1
    end

    # Checks for valid values
    def check_values(value_data)
        # Check if data is a Hash or index-based structure
        unless value_data.is_a?(Hash) || (value_data.is_a?(Array) && value_data.all? {|k,v| k.is_a?(Integer) })
            add_error("Values must be a Hash or indexed Array")
            return -1
        end
    
        # Verify column count matches
        if @table_data && @table_data[:headers]
            expected_count = @table_data[:headers].size
            if value_data.is_a?(Hash)
                # For Hash, check if all keys match table headers
                unless (value_data.keys - @table_data[:headers]).empty?
                    add_error("Column names don't match table headers")
                    return -2
                end
            else
                # For Array, check element count
                unless value_data.size == expected_count
                    add_error("Expected #{expected_count} values, got #{value_data.size}")
                    return -3
                end
            end
        end
    
        # Check for nil values
        if value_data.values.any?(&:nil?)
            add_error("Nil values not allowed")
            return -4
        end
    
        return 0
    end

    # Receive data (a hash of data on format (key => value))
    def values(data)
    #   check current sqlite request
        if check_sqlite_statement("VALUE") == 4
            if check_values(data) == 0
                @insert_values_data = data
                add_request_queue("VALUE")
                self
            end
        else
            add_error("invalid values statement - [not enough values current/required]")
            return -1
        end
    end
    
    def update(table_name)
        table_name = normalize_table_name(table_name)
        
        if !check_filename(table_name)
            return -1
        end

        if check_sqlite_statement("UPDATE") == -2
            return -2
        end

        add_request_queue("UPDATE")
        @table_data = set_table_data(table_name)
        self
    end

    # Update which will receive data (a hash of data on format (key => value))
    def set(data)
        if check_sqlite_statement("SET") == -7
            return -7
        end 
        @update_data = data
        add_request_queue("SET")
        self
    end

    # Set the request to delete on all matching rows
    def delete()
        if check_sqlite_statement("DELETE") == -8
            return -8
        end
        add_request_queue("DELETE")
        self
    end

    # Execute the request
    def run()
        execute_requests()
    end

    def execute_requests()
        return "request queue - empty" if check_for_error

        @request_result = [] # Initialize results storage

        @request_queue.each do |operation|
            #   iterate through the queue and execute each request
            case operation
            when "SELECT"
                @request_result = run_select() || []
            when "UPDATE" 
                next unless @update_data
                run_update()
            when "SET" then next
            when "DELETE" 
                run_delete()
            when "WHERE" then next
            when "FROM"
                @request_result = run_from() || []
                # puts @queue_result
                next
            when "JOIN"
                @request_result = run_join() || []
            when "ORDER" then next
            when "INSERT"
                if @request_queue.include?("VALUE")
                    run_insert()
                end
            when "VALUE" then next
            else
                add_error("Unknown operation: #{operation}")
            end
        end

        # Return results or errors
        check_for_error ? get_request_result : get_request_errors
        @request_result.empty? ? "No results found" : @request_result
    end  

    def run_update()
        return unless @table_data && @update_data
    
        headers = get_table_headers
        updated = false
    
        @table_data[:data].each do |row|
            if @where_result && row[headers.index(@where_result[:column])] == @where_result[:value]
                @update_data.each do |col, val|
                    col_index = headers.index(col)
                    row[col_index] = val if col_index
                end
                updated = true
            end
        end
    
        if updated
            save_table()
            puts "Update successful"
        else
            puts "No rows matched the WHERE condition"
        end
    end

    # Helper function used to save table_data after set and update operations
    def save_table()
        return unless @table_data
        
        CSV.open(@table_data[:name], "w") do |csv|
            csv << get_table_headers
            @table_data[:data].each do |row|
                if row != @table_data[:headers]
                    csv << row
                end
            end
        end
    end
      
    def run_delete()
        unless @table_data
            add_error("No table specified for DELETE")
            return -1
        end

        headers = get_table_headers
        original_count = @table_data[:data].size
        new_data = []

        # Delete rows matching Where condition
        if @where_result
            column_index = headers.index(@where_result[:column])
            unless column_index
                add_error("Column not found: #{@where_result[:column]}")
                return -1
            end

            @table_data[:data].each do |row|
                # Store data that doesn't match into new array
                unless row[column_index] == @where_result[:value]
                    new_data << row
                end
            end
        else
            new_data = [headers] # Delete all but headers
        end

        deleted_count = original_count - new_data.size
        @table_data[:data] = new_data

        if save_table()
            puts "DELETE successful - removed #{deleted_count} rows"
            return deleted_count
        else
            add_error("DELETE failed - could not save table")
            return -1
        end
    end

    def run_from()
        if @table_data && @table_data[:data]
            # Keep original array format in @table_data
            array_data = @table_data[:data]
            
            # Convert to hashes for display
            headers = @table_data[:headers]
            hash_data = array_data.map do |row|
                headers.each_with_index.each_with_object({}) do |(header, index), hash|
                    hash[header] = row[index]
                end
            end
            
            # Return hash format for display, keep array format in @table_data
            return hash_data
        else
            add_error("missing FROM or table data not loaded")
            return nil
        end
    end

    def run_select()
        return nil unless @selected_columns && @table_data
    
        headers = get_table_headers
        data = @table_data[:data]

        # Apply filtering for WHERE if present
        if @where_result
            column_index = headers.index(@where_result[:column])
            if column_index
                data = data.select do |row|
                    row[column_index].to_s == @where_result[:value].to_s
                end
            end
        end
        
        # Convert data rows to hashes with header keys
        data.map do |row|
            @selected_columns.each_with_object({}) do |col_index, hash|
                header = headers[col_index]
                hash[header] = row[col_index]
            end
        end
    end

    def run_insert()
        return -1 unless @table_data && @insert_values_data
      
        unless File.exist?(@table_data[:name]) && File.writable?(@table_data[:name])
            add_error("File not found or not writable")
            return -1
        end

        headers = @table_data[:headers]
    
        # Prepare the new row data
        new_row = if @insert_values_data.is_a?(Hash) && @insert_values_data.keys.all? {|k| k.is_a?(Integer)}
                    # Handle index-based values
                    headers.each_with_index.map { |_, i| @insert_values_data[i] }
                else
                    # Handle hash with column names
                    headers.map { |h| @insert_values_data[h] }
                end
        
        CSV.open(@table_data[:name], 'a') do |csv|
            csv << new_row
        end

        return 0
    end

    def run_join()
        return -1 unless @table_data && @join_data
    
        table_a = @table_data
        table_b = set_table_data(@join_data[:table_b])
    
        # Get column indices
        col_a = table_a[:headers].index(@join_data[:column_a])
        col_b = table_b[:headers].index(@join_data[:column_b])
    
        # Prepare combined headers
        combined_headers = table_a[:headers] + table_b[:headers]
    
        # Perform the join and convert to array of hashes
        joined_data = []
        table_a[:data].each do |row_a|
            table_b[:data].each do |row_b|
                if row_a[col_a] == row_b[col_b]
                    # Combine rows and convert to hash
                    combined_row = {}
                    row_a.each_with_index do |val, i|
                        combined_row[table_a[:headers][i]] = val
                    end
                    row_b.each_with_index do |val, i|
                        combined_row[table_b[:headers][i]] = val
                    end
                    joined_data << combined_row
                end
            end
        end
    
        # Update table data structure
        @table_data = {
            name: table_a[:name],
            headers: combined_headers,
            data: joined_data.map { |h| combined_headers.map { |header| h[header] } }
        }
    
        # Debug
        p joined_data

        # Return formatted results for display
        return joined_data
    end
end