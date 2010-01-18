require 'rubygems'
require 'sequel'
require 'worker'

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
        @DB_from, @DB_to = Sequel.connect(from.merge(:max_connections => 5, :single_threaded => false)),
                Sequel.connect(to.merge(:max_connections => 5, :single_threaded => false))
        #@DB_from, @DB_to = Sequel.connect(from.merge(:single_threaded => true)), Sequel.connect(to.merge(:single_threaded => true))
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
      threads = []
      multi_threaded = !(@DB_from.single_threaded? || @DB_to.single_threaded?)
      @tables_to_copy.each do |tab|
        db_src_conn, db_target_con, obj_to_notify, table_to_copy, copy_columns_for_table =
                @DB_from, @DB_to, self, tab.to_sym, self.copy_columns[tab.to_sym]
        if multi_threaded
          t = Thread.new(table_to_copy, db_src_conn, db_target_con, obj_to_notify, copy_columns_for_table) do
            $stderr.puts "starting Thread: #{Thread.current.object_id}"
            w = Worker.new :src_db_conn => db_src_conn, :target_db_conn => db_target_con,
                           :table_name => table_to_copy, :copy_columns => copy_columns_for_table, :notify => obj_to_notify
            w.copy_table
          end
          threads << t
        else
          $stderr.puts "running in single threaded mode"
          w = Worker.new :src_db_conn => db_src_conn, :target_db_conn => db_target_con,
                         :table_name => table_to_copy, :copy_columns => copy_columns_for_table, :notify => obj_to_notify
          w.copy_table
        end
      end
      threads.each {|t| t.join} if multi_threaded
    end

    protected :copy_tables

    def copy_complete(thread)
      $stderr.puts "#{thread.object_id} has completed it's copy"
    end

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

