require 'csv'

class chain_method
    def initialize
        @temp = []
        @file_data = {}
    end

    def get_table_headers(table_data)
        return table_data[0]
    end

    def read_csv_file(file_name)
        return CSV.read(file_name, converters: :all)
    end

    def get_table_data(table_name)
        table_data = {
            name: table_name,
            data: read_csv_file(table_name),
        }
        table_hash[:headers] = get_table_headers(table_data[:data])
        return table_data
    end
    # def set_table_data(name)
    #     @file_data[:name] = name
    #     @file_data[:header] = #first row
    #     @file_data[:data] = #remaining rows
    # end

    def append(row)
        @temp.append(row)
    end

    #   not sure if needed
    def read
        @temp
    end

    #   from some csv - testing will assume perfect file
    def from(file_name)
        
    end

    def select

    end

    def append

    end

    def update

    end

    def to_s
        @temp
    end


end