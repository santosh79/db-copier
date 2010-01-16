require 'rubygems'
require 'sequel'
require 'json'

module DbCopier
  class Application
    attr_reader :tables_to_copy
    def initialize &block
      @tables_to_copy        = []
      @index_to_copy         = []
      @only_tables_to_copy   = []
      @except_tables_to_copy = []
      instance_eval &block
    end

    def copy options = {}
      begin
        from, to, @rows_per_copy = options[:from], options[:to], (options[:rows_per_copy] || 50)
        raise ArgumentError unless from && to && from.is_a?(Hash) && to.is_a?(Hash) && from.size > 0 && to.size > 0
        @DB_from , @DB_to = Sequel.connect(from), Sequel.connect(to)
        @DB_from.test_connection && @DB_to.test_connection
        @tables_to_copy = @DB_from.tables
        @DB_to.tables
        instance_eval { yield } if block_given?
        if options[:mock_copy]
          mock_copy_tables
        else
          copy_tables
        end
      ensure
        self.close_connections
      end
    end

    def index(ind)
      @index_to_copy << ind
    end

    def only(*tabs)
      @tables_to_copy = Array(tabs)
    end

    def except(*tabs)
      @tables_to_copy -= Array(tabs).map { |tb| tb.to_sym }
    end

    def copy_tables
      @tables_to_copy.each do |tab|
        tab_to_copy = tab.to_sym
        puts "tab is #{tab_to_copy}"
        num_rows = @DB_from[tab_to_copy].count
        #num_rows = 1000
        i = 1
        table_schema = @DB_from.schema(tab_to_copy)
        #Create the table if it does not already exist
        #unless @DB_to.table_exists?
        #  cols_string =
        #  table_creation_string = %{
        #
        #
        #  }
        #  @DB_to.create_table tab_to_copy do
        #
        #  end
        #end
        date_time_columns = []
        table_schema.each do |column_schema|
          if column_schema[1][:type] == :datetime
            date_time_columns << column_schema.first
          end
        end
        while i < num_rows
          rows_to_copy = @DB_from[tab_to_copy].limit(@rows_per_copy, i).all
          #Special handling of datetime columns
          rows_to_copy.each { |row| date_time_columns.each {|dt_col| row[dt_col] = DateTime.parse(row[dt_col].inspect)} }

          puts "copying rows: #{i}-#{i+(rows_to_copy.count)} for table: #{tab_to_copy}"
          i += rows_to_copy.count
          @DB_to[tab_to_copy].multi_insert(rows_to_copy)
        end
      end
    end

    def mock_copy_tables
      @tables_to_copy.each do |tab|
        puts "copying table #{tab}"
        indexes_for_table = @DB_from.indexes(tab)
        indexes_for_table.keys.each do |inx|
          #puts "copying index #{inx[:columns]}"
          puts "copying index #{inx} with columns #{indexes_for_table[inx][:columns]}"
        end
      end
    end
    protected :copy_tables

    def close_connections
      puts "closing connections"
      @DB_from.disconnect if defined?(@DB_from) && @DB_from
      @DB_to.disconnect if defined?(@DB_to) && @DB_to
    end
    protected :close_connections

  end

  def self.app &block
    Application.new &block
  end

  
end

