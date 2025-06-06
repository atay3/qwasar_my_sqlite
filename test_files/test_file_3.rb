require_relative '../my_sqlite_request'
require_relative '../my_sqlite_cli'


puts "BEFORE UPDATE:"
CSV.foreach("student.csv", headers: true) do |row|
  puts row.to_h
end

# request = MySqliteRequest.new
# request.update("student")
#        .where("name", "a")
#        .set({ "id" => "7", "age" => "11" })
#        .run()

# For INSERT
request = MySqliteRequest.new
request.insert('student')
      .values({name: 'Alice', age: 12, id: 3})
      .run

# For JOIN
# request = MySqliteRequest.new
# request.from('student')
#        .join('id', 'grades.csv', 'student_id')
#        .run

puts "\nAFTER UPDATE:"
CSV.foreach("student.csv", headers: true) do |row|
  puts row.to_h
end

# request = MySqliteRequest.new
# request.from("student.csv")
#        .delete()
#        .run()