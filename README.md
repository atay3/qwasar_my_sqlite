# Welcome to My Sqlite
***

## Task
This projects consists of the MySqliteRequest class which behaves similarly to the real sqlite. It contains basic functionalities for select, from, join, where, order, insert, values, update, set, and delete. In addition to this class, there is a CLI (Command Line Interface) that uses readline. The program can save and load a database from the file. It accepts requests with the following:

SELECT|INSERT|UPDATE|DELETE
FROM
WHERE (max 1 condition)
JOIN ON (max 1 condition)

## Description
This project implements a simple version of a SQLite database system in Ruby, consisting of two main components: the MySqliteRequest class (my_sqlite_request.rb) which handles core database operations, and a CLI that allows users to send queries. The solution approaches the problem by implementing a mostly chainable request builder that processes CSV files as database tables.

The MySqliteRequest class provides the fundamental database operations. It uses a request queue system that validates and executes operations in sequence. The implementation reads and writes CSV files directly, treating each file as a database table with the first row as column headers. For JOIN operations, the system performs in-memory table combinations by matching specified columns.

The CLI has an interactive shell with readline support for query parsing. It translates user input into method calls on the MySqliteRequest instance, handling both simple and complex queries.

Error handling is implemented throughout both components, with validation for tables, column references, and query structure. While not a full SQL implementation, it provides the essential features needed for basic database operations.

## Installation
Clone this repo, then follow Usage guide:
```bash
https://git.us.qwasar.io/my_sqlite_183544_cc1con/my_sqlite.git
```

## Usage
Compile and run the project by running
```
ruby my_sqlite_cli.rb
```

Below are basic commands that can be used.

### Select
```
SELECT * FROM student.csv
SELECT name, age FROM student.csv WHERE age > 20
```
### Insert
```
INSERT student.csv VALUES (1, 'John Doe', 22)
```
### Update
```
UPDATE student.csv SET age = 23 WHERE name = 'John Doe'
```
### Delete
```
DELETE student.csv WHERE id = 1
```
### Join
```
SELECT * FROM student.csv JOIN grade.csv ON student.id = grade.student_id
```
### Order
```
SELECT * FROM student.csv ORDER age DESC
```



### The Core Team


<span><i>Made at <a href='https://qwasar.io'>Qwasar SV -- Software Engineering School</a></i></span>
<span><img alt='Qwasar SV -- Software Engineering School's Logo' src='https://storage.googleapis.com/qwasar-public/qwasar-logo_50x50.png' width='20px' /></span>
