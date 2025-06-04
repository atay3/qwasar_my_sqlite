require_relative 'my_sqlite_request'
require 'readline'

class MySqliteCli
    def initialize
        @request = MySqliteRequest.new
    end

    def process_input(input)
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
        # Parse and execute the query
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

        # Parse query
        if match = query.match(/SELECT (.+?) FROM (\w+)(?: WHERE (.+))?(?: JOIN (.+))?(?: ORDER BY (.+))?/i)
            cols = match[1].strip
            table = match[2]
            where_clause = match[3]
            join_clause = match[4]
            order_clause = match[5]
      
            @request.from(table)
            handle_select_columns(cols)
            handle_where(where_clause) if match[3]
            handle_join(join_clause) if match[4]
            handle_order(order_clause) if match[5]
      
            display_results(@request.run)
        else
            puts "Error: Invalid SELECT syntax."
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
        # Parse query
        if match = where_clause.match(/(\w+)\s*=\s*(?:'([^']+)'|"([^"]+)"|(\S+))/)
            column = match[1]
            value = match[2] || match[3] || match[4]
            # p value
            @request.where(column, value)
        else
            puts "Error: Invalid format."
        end
    end

    def handle_update(query)
        return if @request.nil?

        if match = query.match(/UPDATE\s+(\w+)\s+SET\s+(.+?)(?:\s+WHERE\s+(.+))?$/i)
            table = match[1].strip
            set_assignments = match[2]
            where_clause = match[3]

            # p table
            # p set_assignments
            # p where_clause
      
            if @request.update(table) < 0
                add_error("Update failed")
                false
            else
                handle_set(set_assignments)
                handle_where(where_clause) if where_clause
                @request.run()
            end
        else
            puts "Error: Invalid UPDATE syntax."
        end
    end

    def handle_insert(query)
    end

    def handle_delete(query)
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
        if match = join_clause.match(/JOIN (\w+)\s+ON\s+(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)/i)
            table_b = match[1]
            table_a_col = match[3]
            table_b_col = match[5]
            
            # Verify tables and columns exist
            unless File.exist?(table_b)
              add_error("Table #{table_b} not found")
              return false
            end
            
            if @request.join(table_a_column, table_b, table_b_column) == 0
                true
            else
                add_error("Join failed")
                false
            end
        else
            add_error("Invalid JOIN syntax. Use: JOIN table ON table1.column = table2.column")
            false
        end
    end

    def handle_order(order_clause)
        if match = order_clause.match(/ORDER BY (\w+)(?:\s+(ASC|DESC))?/i)
            column = match[1]
            direction = match[2] ? match[2].downcase.to_sym : :asc # Default to ASC
            
            # Validate direction
            unless [:asc, :desc].include?(direction)
                add_error("Invalid ORDER BY direction")
                return false
            end
            
            if @request.order(order_direction, column) == 0
                true
            else
                add_error("Order failed")
                false
            end
        else
            add_error("Invalid ORDER BY syntax. Use: ORDER BY column [ASC|DESC]")
            false
        end
    end

    def display_results(results)
        case results
        when String
            puts results  # Print the message
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
        # when nil
        #     puts "No results found"
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