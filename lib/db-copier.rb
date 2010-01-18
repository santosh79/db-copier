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
        copy_tables
      ensure
        self.close_connections
      end
    end

    def copy_columns
      #Hash of the form {:table => [:col1, :col2]} -- maintaing record of columns to keep for a table
      @copy_columns ||= {}
    end
    protected :copy_columns

    def for_table(table, options = {})
      raise ArgumentError, "missing required copy_cols attribute" unless (copy_columns = options[:copy_columns])
      table, copy_columns = table.to_sym, copy_columns.map {|col| col.to_sym}
      raise ArgumentError, "columns do not exist" unless (@DB_from.schema(table).map {|cols| cols.first} & copy_columns) == copy_columns
      self.copy_columns[table] = copy_columns
      #puts "here are the copy columns: #{self.copy_columns.inspect}"
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
        num_rows = @DB_from[tab_to_copy].count
        i = 0
        #Create the table if it does not already exist
        unless @DB_to.table_exists?(tab_to_copy)
          table_creation_ddl = DbCopier.generate_create_table_ddl(@DB_from, tab_to_copy, tab_to_copy, (self.copy_columns[tab_to_copy] || []))
          table_creation_ddl = ("@DB_to" + table_creation_ddl)
          eval table_creation_ddl
        end

        #This is the intersection of columns specified via the +copy_columns+ argumnent in the +for_table+ method
        #and those that actually exist in the target table.
        columns_in_target_db = @DB_to.schema(tab_to_copy).map {|cols| cols.first}
        columns_to_copy = columns_in_target_db & (copy_columns[tab_to_copy] || columns_in_target_db)
        #puts "copy_columns[tab_to_copy]: #{copy_columns[tab_to_copy]}\t@DB_to.schema(tab_to_copy)#{@DB_to.schema(tab_to_copy).map {|cols| cols.first}.inspect}\tcolumns_to_copy: #{columns_to_copy.inspect}"
        while i < num_rows
          rows_to_copy = @DB_from[tab_to_copy].select(*columns_to_copy).limit(@rows_per_copy, i).all
          #Special handling of datetime columns
          rows_to_copy.each { |col_name, col_val| rows_to_copy[col_name] = DateTime.parse(col_val) if col_val.class == Time }
          i += rows_to_copy.count
          @DB_to[tab_to_copy].multi_insert(rows_to_copy)
        end

        #copy indexes now
        @DB_from.indexes(tab_to_copy).each do |index_name, index_info|
          #Make sure we are adding an index to a column that is going to be there
          next unless (columns_to_copy & index_info[:columns] == index_info[:columns])
          @DB_to.add_index(tab_to_copy, index_info[:columns])
        end
      end
    end

    protected :copy_tables

    def close_connections
      @DB_from.disconnect if defined?(@DB_from) && @DB_from
      @DB_to.disconnect if defined?(@DB_to) && @DB_to
    end

    protected :close_connections

  end

  def self.app &block
    Application.new &block
  end

  def self.generate_create_table_ddl(db_conn, table_to_create_schema_from, new_table_name, only_copy_columns = [])
    ret = String.new
    db_conn.schema(table_to_create_schema_from.to_sym).each do |col|
      col_name, col_type = col[0].to_sym, col[1][:type]
      #Skip creating this column if it was not specified in the columns to copy for this table
      next if only_copy_columns.count > 0 && !only_copy_columns.include?(col_name)
      if col[1][:primary_key]
        ret << "primary_key #{col_name.inspect}, #{col_type.to_sym.inspect}, :default => #{col[1][:default].inspect}, :null => #{col[1][:allow_null].inspect};\n"
      else
        ret << "#{col_type.to_s.capitalize} #{col_name.inspect}, :default => #{col[1][:default].inspect}, :null => #{col[1][:allow_null].inspect};\n"
      end
    end
    ret = (".create_table #{new_table_name.to_sym.inspect} do \n" + ret)
    ret << "end\n"
    #puts "here is the ddl #{ret}"
    #ret
  end

end

