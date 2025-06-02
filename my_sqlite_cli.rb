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
        # @request = MySqliteRequest.new
        # Parse and execute the query
        case query
        when /^SELECT/ then handle_select(query)
        # when /^JOIN/ then handle_join(query)
        # when /^ORDER/ then handle_order(query)
        when /^UPDATE/ then handle_update(query)
        # when /^SET/ then handle_set(query)
        # when /^INSERT/i then handle_insert(query)
        # when /^DELETE/i then handle_delete(query)
        else
            puts "Error: unsupported query"
        end
        
    end

    def handle_select(query)
        return if @request.nil?

        if match = query.match(/SELECT\s+(.+?)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?/i)
            # Parse query
            cols = match[1].strip
            table = match[2]
            where_clause = match[3]

            # p cols
            # p table
            # p where_clause
      
            @request.from(table)
            handle_select_columns(cols)
            handle_where(where_clause) if where_clause
      
            display_results(@request.run)
        else
            puts "Error: Invalid syntax."
        end
    end

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
            # Parse query
            table = match[1].strip
            set_assignments = match[2]
            where_clause = match[3]

            p table
            p set_assignments
            p where_clause
      
            @request.update(table)
            handle_set(set_assignments)
            handle_where(where_clause) if where_clause
      
            display_results(@request.run)
        else
            puts "Error: Invalid syntax."
        end
    end

    def handle_set(set_assignments)
        set_assignments.split(',').each do |assignment|
            # col, val = assignment.split('=').map(&:strip)
            # @request.set({col => val})
            if assignment.match(/(\w+)\s*=\s*(?:'([^']+)'|"([^"]+)"|(\S+))/)
                col = $1
                val = $2 || $3 || $4
                @request.set(col => val)
            end
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
        when nil
            puts "No results found"
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