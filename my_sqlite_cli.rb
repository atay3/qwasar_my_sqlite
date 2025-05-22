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
        # when /^UPDATE/ then handle_update(query)
        # when /^INSERT/i then handle_insert(query)
        # when /^DELETE/i then handle_delete(query)
        end
        # Handle wildcard
        # Handle reqests for specific columns
    end

    def handle_select(query)
        # if query.match(/SELECT \* FROM (\w+)/i)
        #     table = $1
        #     request.from("#{table}.csv").select('*')
        #     results = request.run
        #     display_results(results)
        # end
    end

    if __FILE__ == $PROGRAM_NAME
        # Get filename from ARGV
        table_name = ARGV[0]&.gsub(/\.csv$/, '')
        
        # Run CLI
        cli = MySqliteCli.new()
        cli.run_prompt
    end
end