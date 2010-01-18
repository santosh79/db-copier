module DbCopier
  class Worker
    def initialize(options = {})
      @src_db_conn, @target_db_conn, @table_name, @rows_per_copy, @copy_columns = options[:src_db_conn], options[:target_db_conn],
              options[:table_name], (options[:rows_per_copy] || 1000), (options[:copy_columns] || [])
      raise ArgumentError unless @src_db_conn && @target_db_conn && @table_name
      @copy_columns ||= []
    end

    def copy_table
      tab_to_copy = @table_name.to_sym
      num_rows = @src_db_conn[tab_to_copy].count
      i = 0
      #Create the table if it does not already exist
      unless @target_db_conn.table_exists?(tab_to_copy)
        table_creation_ddl = DbCopier.generate_create_table_ddl(@src_db_conn, tab_to_copy, tab_to_copy, @copy_columns)
        table_creation_ddl = ("@target_db_conn" + table_creation_ddl)
        eval table_creation_ddl
      end

      #This is the intersection of columns specified via the +copy_columns+ argumnent in the +for_table+ method
      #and those that actually exist in the target table.
      columns_in_target_db = @target_db_conn.schema(tab_to_copy).map {|cols| cols.first}
      columns_to_copy =
              if @copy_columns.count > 0
                @copy_columns & columns_in_target_db
              else
                columns_in_target_db
              end

      #puts "copy_columns[tab_to_copy]: #{@copy_columns.inspect}\tcolumns_in_target_db#{columns_in_target_db.inspect}\tcolumns_to_copy: #{columns_to_copy.inspect}"
      while i < num_rows
        rows_to_copy = @src_db_conn[tab_to_copy].select(*columns_to_copy).limit(@rows_per_copy, i).all
        #Special handling of datetime columns
        rows_to_copy.each { |col_name, col_val| rows_to_copy[col_name] = DateTime.parse(col_val) if col_val.class == Time }
        i += rows_to_copy.count
        @target_db_conn[tab_to_copy].multi_insert(rows_to_copy)
      end

      #copy indexes now
      @src_db_conn.indexes(tab_to_copy).each do |index_name, index_info|
        #Make sure we are adding an index to a column that is going to be there
        next unless (columns_to_copy & index_info[:columns] == index_info[:columns])
        @target_db_conn.add_index(tab_to_copy, index_info[:columns])
      end
    end
  end
end
