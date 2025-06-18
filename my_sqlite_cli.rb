require_relative 'my_sqlite_request'
require 'readline'

class MySqliteCli
    def initialize
        @request = MySqliteRequest.new
    end

    def process_input(input)
        # Create new reqeust for each query
        @request = MySqliteRequest.new if @request
        case input.strip.downcase
        when 'exit', 'quit'
            exit 0
        when ''
            return
        else
            execute_query(input)
        end
    end

    def execute_query(query)
        case query
        when /^SELECT/ then handle_select(query)
        when /^UPDATE/ then handle_update(query)
        when /^INSERT/i then handle_insert(query)
        when /^DELETE/i then handle_delete(query)
        else
            puts "Error: unsupported query"
        end
    end

    def handle_select(query)
        return if @request.nil?
    
        # Get SELECT and FROM
        if base_match = query.match(/SELECT\s+(?<cols>.+?)\s+FROM\s+(?<table>[\w\.]+)/i)
            cols = base_match[:cols].strip
            table = base_match[:table].strip
            
            @request.from(table)
            handle_select_columns(cols)
    
            # Process each clause type independently
            handle_where(query)
            handle_join(query)
            handle_order(query)
            
            display_results(@request.run)
        else
            puts "Error: Invalid SELECT syntax"
        end
    end

    # Helper function for handle_select
    def handle_select_columns(cols)
        # Handle wildcard and requests for specific columns
        if cols == '*'
            @request.select('*')
        else
            cols.split(',').each do |col|
                @request.select(col.strip)
            end
        end
    end

    def handle_where(where_clause)
        unless where_clause&.include?("WHERE")
            return false
        end

        # Parse query
        if match = where_clause.match(/(\w+)\s*=\s*(?:'([^']+)'|"([^"]+)"|(\S+))/)
            column = match[1]
            value = match[2] || match[3] || match[4]
            # p value
            @request.where(column, value)
            return true
        else
            puts "Error: Invalid format"
            return false
        end
    end

    def handle_update(query)
        return if @request.nil?

        if match = query.match(/UPDATE\s+([\w\.]+)\s+SET\s+(.+?)(?:\s+WHERE\s+(.+))?$/i)
            table = match[1].strip
            set_assignments = match[2]
            where_clause = match[3]
      
            @request.update(table)
            handle_set(set_assignments)
            handle_where(where_clause) if where_clause
            
            @request.run()
        else
            puts "Error: Invalid UPDATE syntax"
        end
    end

    def handle_insert(query)
        unless query&.include?("INSERT")
            return false
        end

        pattern = %r{
            INSERT\s+([\w\.]+)\s*
            (?:\(\s*([^)]+)\s*\)\s*)?
            VALUES\s*\(\s*([^)]+)\s*\)
        }ix

        if match = query.match(pattern)
            table = match[1]
            columns = match[2] ? match[2].split(',').map(&:strip) : nil
            values = match[3].split(',').map(&:strip).map { |v| v.gsub(/^['"]|['"]$/, '') }
        
            # Create data hash (match columns with values)
            data = if columns
                    columns.zip(values).to_h
                else
                    values.each_with_index.to_h { |v, i| [i, v] }
                end
            
            if @request.insert(table) == 0
                @request.values(data).run
            else
                puts "Insert failed"
            end            
        else
            puts "Error: Invalid INSERT syntax"
        end
    end

    def handle_delete(query)
        return if @request.nil?

        if match = query.match(/DELETE\s+([\w\.]+)(?:\s+WHERE\s+(.+))?/i)
            table = match[1]
            where_clause = match[2]

            @request.from(table)
            @request.delete()

            handle_where(where_clause) if where_clause

            @request.run
        else
            puts "Error: Invalid DELETE syntax"
        end
    end

    def handle_set(set_assignments)
        set_data = {}
        set_assignments.split(',').each do |assignment|
            col, val = assignment.split('=').map(&:strip)
            set_data[col] = val.gsub(/['"]/, '')
        end
        @request.instance_variable_set(:@update_data, set_data)
    end

    def handle_join(join_clause)
        # Exit early if no JOIN clause is present
        unless join_clause&.include?("JOIN")
            puts "No JOIN clause found"
            return false
        end

        if match = join_clause.match(/JOIN\s+([\w\.]+)\s+ON\s+(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)/i)
            table_b_file = match[1]
            table_a_col = match[3]
            table_b_col = match[5]
            
            # Verify tables and columns exist
            unless File.exist?(table_b_file)
                puts "Table #{table_b_file} not found"
                return false
            end

            # Set join data
            @request.instance_variable_set(:@join_data, {
                table_b: table_b_file,
                column_a: table_a_col,
                column_b: table_b_col
            })
            
            if @request.join(table_a_col, table_b_file, table_b_col) == 0
                return true
            else
                puts "Join failed"
                return false
            end
        else
            puts "Invalid JOIN syntax"
            return false
        end
    end

    def handle_order(order_clause)
        unless order_clause&.include?("ORDER")
            return false
        end

        if match = order_clause.match(/ORDER\s+(\w+)(?:\s+(ASC|DESC))?/i)
            column = match[1]
            direction = match[2] ? match[2].downcase.to_sym : :asc # Default to ASC
            
            # Validate direction
            unless [:asc, :desc].include?(direction)
                puts "Invalid ORDER direction"
                return false
            end
            
            if @request.order(direction, column) == 0
                return true
            else
                puts "Order failed"
                return false
            end
        else
            puts "Invalid ORDER syntax"
            return false
        end
    end

    def display_results(results)
        case results
        when String
            puts results
        when Array
            if results.empty?
                puts "No results found"
            else
                cols = results.first.keys
                puts cols.join(" | ")
                puts "-" * cols.sum(&:length) + "-" * (cols.size * 3)
                results.each do |row|
                    puts cols.map { |col| row[col].to_s }.join(" | ")
                end
            end
        end
    end

    def run_prompt()
        puts "MySQLite version 0.1 20XX-XX-XX"
        while input = Readline.readline("my_sqlite_cli> ", true)
            process_input(input)
        end
    end

    if __FILE__ == $PROGRAM_NAME
        # Get filename from ARGV
        table_name = ARGV[0]&.gsub(/\.csv$/, '')
        
        # Run CLI
        cli = MySqliteCli.new()
        cli.run_prompt
    end
end