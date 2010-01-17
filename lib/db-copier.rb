require 'rubygems'
require 'sequel'

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
        @DB_from, @DB_to = Sequel.connect(from), Sequel.connect(to)
        @DB_from.test_connection && @DB_to.test_connection #test connections
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

    def copy_columns
      #Hash of the form {:table => [:col1, :col2]} -- maintaing record of columns to keep for a table
      @copy_columns ||= {}
    end

    def for_table(table, options = {})
      raise ArgumentError, "missing required copy_cols attribute" unless (copy_columns = options[:copy_columns])
      table, copy_columns = table.to_sym, copy_columns.map {|col| col.to_sym}
      raise ArgumentError, "table does not exist" unless @DB_to.table_exists?(table)
      raise ArgumentError, "columns do not exist" unless (@DB_to.schema(table).map {|cols| cols.first} & copy_columns) == copy_columns
      self.copy_columns[table] = copy_columns
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
        $stderr.puts "num_rows is: #{num_rows}"
        #num_rows = 1000
        i = 0
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

        #This is the intersection of columns specified via the +copy_columns+ argumnent in the +for_table+ method
        #and those that actually exist in the target table.
        columns_to_copy = (copy_columns[tab_to_copy] || []) | (@DB_to.schema(tab_to_copy).map {|cols| cols.first})
        while i < num_rows
          rows_to_copy = @DB_from[tab_to_copy].select(*columns_to_copy).limit(@rows_per_copy, i).all
          #Special handling of datetime columns
          rows_to_copy.each { |col_name, col_val| rows_to_copy[col_name] = DateTime.parse(col_val) if col_val.class == Time }
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

