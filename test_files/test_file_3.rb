require_relative '../my_sqlite_request'
require_relative '../my_sqlite_cli'


# Print the CSV before update
# puts "BEFORE UPDATE:"
# CSV.foreach("test_data/student.csv", headers: true) do |row|
#   puts row.to_h
# end

#debug start - update warren
# request = MySqliteRequest.new
# puts "start\n"
# request.update("../test_data/student.csv")
#debug end warren

# Initialize and build the update request
request = MySqliteRequest.new
request = request.from("nba_players.csv")
request = request.select("test")
p request
# request = request.from("nba_players.csv")
# p "from #{request}"
# p "#{request.get_request_errors}"
# result = request.run
# p result

# request.update("student")
#        .set({ "ID" => "7", "AGE" => "11" })
#        .where("NAME", "a")
# request.run



# ---- BEGIN mock .run implementation for testing ----
# simulate the update logic since `.run` is not implemented

# Load CSV
# table_data = CSV.table("student.csv", headers: true)

# # Apply WHERE clause manually (matches NAME == "a")
# table_data.each do |row|
#   if row["NAME"] == "a"
#     request.instance_variable_get(:@set_values).each do |col, val|
#       row[col] = val
#     end
#   end
# end

# # Write updated CSV back
# CSV.open("student.csv", "w", write_headers: true, headers: table_data.headers) do |csv|
#   table_data.each { |row| csv << row }
# end
# # ---- END mock run ----

# # Print the CSV after update
# puts "\nAFTER UPDATE:"
# CSV.foreach("student.csv", headers: true) do |row|
#   puts row.to_h
# end