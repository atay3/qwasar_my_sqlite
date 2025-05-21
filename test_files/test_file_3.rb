require_relative '../my_sqlite_request'
require_relative '../my_sqlite_cli'


# Print the CSV before update
puts "BEFORE UPDATE:"
CSV.foreach("student.csv", headers: true) do |row|
  puts row.to_h
end

# Initialize and build the update request
request = MySqliteRequest.new
request.update("student.csv")
       .where("name", "a")
       .set({ "id" => "7", "age" => "11" })
       .run()

# Print the CSV after update
puts "\nAFTER UPDATE:"
CSV.foreach("student.csv", headers: true) do |row|
  puts row.to_h
end

request = MySqliteRequest.new
request.from("student.csv")
       .delete()
       .run()