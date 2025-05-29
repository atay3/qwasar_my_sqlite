require_relative 'my_sqlite_request'
require 'readline'

class MySqliteCli
    def run_prompt()
        puts "MySQLite version 0.1 20XX-XX-XX"

        while input = Readline.readline("my_sqlite_cli> ", true)
            process_input(input)
        end
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
        request = MySqliteRequest.new
        # Parse and execute the query
        case query
        when /^SELECT/ then handle_select(query)
        # when /^FROM/ then handle_from(query)
        # when /^WHERE/ then handle_where(query)
        # when /^JOIN/ then handle_join(query)
        # when /^ORDER/ then handle_order(query)
        # when /^UPDATE/ then handle_update(query)
        # when /^SET/ then handle_set(query)
        # when /^INSERT/i then handle_insert(query)
        # when /^DELETE/i then handle_delete(query)
        else
            puts "Error: unsupported query"
        end
        
    end

    def handle_select(query)
        # Handle wildcard
        # Handle reqests for specific columns
        
        if query.match(/SELECT (.+?) FROM (\w+)(?: WHERE (.+))?/i)
            table = $2
            p table
            p $1
            request = MySqliteRequest.new
            request.from(table).select($1)
            results = request.run
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