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
            break
        when ''
            next
        else
            execute_query(input)
        end
    end

    def execute_query(query)
        request = MySqliteRequest.new
        # Parse and execute the query
        
        # Handle wildcard

        #Handle reqests for specific columns
    end
end