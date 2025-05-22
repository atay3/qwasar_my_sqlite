require 'my_sqlite_request'
require 'readline'

while buf = Readline.readline(">", true)
    p buf
end