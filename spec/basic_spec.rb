require File.dirname(__FILE__) + "/spec_helper.rb"

describe DbCopier do
  clean_up = lambda {|db_info, table_to_truncate| Sequel.connect(db_info)[table_to_truncate.to_sym].truncate } #Proc to truncate tables

  mock_db = {:adapter => 'mysql', :host => 'localhost', :user => 'root', :password => '', :database => 'db_copier_test'}
  fake_cred_db = {:adapter => 'mysql', :host => 'localhost', :user => 'rootsss', :password => '', :database => 'db_copier_test'}
  source_db = {:adapter => 'mysql', :host => 'localhost', :user => 'root', :password => '', :database => 'dharma_development'}
  target_db = {:adapter => 'mysql', :host => 'localhost', :user => 'root', :password => '', :database => 'admin_tools_development'}

  it "should throw an argument error if the :from and :to hashes are not provided" do
    begin
      DbCopier.app do
        copy
      end
      raise RuntimeError, "shouldn't be here"
    rescue ArgumentError
    end

    begin
      DbCopier.app do
        copy :from => {:adapter => 'mysql'}
      end
      raise RuntimeError, "shouldn't be here"
    rescue ArgumentError
    end

    begin
      DbCopier.app do
        copy :to => {:adapter => 'mysql'}
      end
      raise RuntimeError, "shouldn't be here"
    rescue ArgumentError
    end
  end

  it "should throw an argument error if :from and :to are not hashes" do
    begin
      DbCopier.app do
        copy :from => 'foo', :to => 'bar'
      end
      raise RuntimeError, "shouldn't be here"
    rescue ArgumentError
    end
  end

  it "should throw an Sequel::DatabaseConnectionError for invalid credentials" do
    begin
      DbCopier.app do
        copy :from => mock_db, :to => fake_cred_db
      end
      raise RuntimeError, "shouldn't be here"
    rescue Sequel::DatabaseConnectionError
    end
  end

  it "should copy all tables if no #table methods are called in the dsl" do
    app = DbCopier.app do
      copy :from => source_db,
           :to => mock_db, :mock_copy => true
    end
    ['clicks', 'custom_src_query_ratings', 'invitations', 'result_sets', 'review_votes', 'schema_migrations', 'search_reviews', 'shares', 'source_ratings', 'station_searches', 'station_sources', 'stations', 'suggested_sources', 'twitter_oauth_infos', 'user_custom_src_query_ratings', 'user_source_query_histories', 'users', 'users_followers', 'votes'].each do |tab|
      app.tables_to_copy.should include(tab.to_sym)
    end
  end

  it "should copy only select tables if they are provided as an #only method in the dsl" do
    app = DbCopier.app do
      copy :from => source_db, :to => mock_db, :mock_copy => true do
        only 'custom_src_query_ratings', 'result_sets', 'review_votes'
      end
    end
    app.tables_to_copy.map {|tab| tab.to_s}.sort.should == ['custom_src_query_ratings', 'result_sets', 'review_votes']
  end



  it "should copy only select tables if they are provided as an #only method in the dsl - real test" do
    mock_copy = true
    app = DbCopier.app do
      copy :from => source_db,
           :to => target_db, :mock_copy => mock_copy, :rows_per_copy => 1000 do
        only 'custom_src_query_ratings'
      end
    end
    app.tables_to_copy.map {|tab| tab.to_s}.sort.should == ['custom_src_query_ratings']
    clean_up.call(target_db, 'custom_src_query_ratings') unless mock_copy
  end

  it "should not copy tables specified in the #except dsl method" do
    app = DbCopier.app do
      copy :from => source_db,
           :to => mock_db,
           :mock_copy => true do
        except 'custom_src_query_ratings', 'result_sets', 'review_votes'
      end
    end
    app.tables_to_copy.map {|tab| tab.to_s}.sort.should == ['clicks', 'invitations', 'schema_migrations', 'search_reviews', 'shares', 'source_ratings', 'station_searches', 'station_sources', 'stations', 'suggested_sources', 'twitter_oauth_infos', 'user_custom_src_query_ratings', 'user_source_query_histories', 'users', 'users_followers', 'votes']
  end

  it "should throw an ArgumentError if the for_table method is called without appropriate args" do
    begin
      app = DbCopier.app do
        copy :from => source_db, :to => mock_db, :mock_copy => true do
          except 'custom_src_query_ratings', 'result_sets', 'review_votes'
          for_table :custom_src_query_ratings #Missing :copy_columns argument
        end
      end
      raise RuntimeError, "Shouldn't be here"
    rescue ArgumentError
    end
  end

  it "should only copy the columns specified in the +copy_columns+ argument in the for_table method" do
    app = DbCopier.app do
      copy :from => source_db, :to => target_db, :mock_copy => true do
        except 'custom_src_query_ratings', 'result_sets', 'review_votes'
        for_table :custom_src_query_ratings, :copy_columns => ['id', 'source_id']
      end
    end
    app.copy_columns[:custom_src_query_ratings].should == [:id, :source_id]
  end

  it "should throw an ArgumentError if the table specified in the for_table method does not exist" do
    begin
      app = DbCopier.app do
        copy :from => source_db,
             :to => mock_db, :mock_copy => true do
          except 'custom_src_query_ratings', 'result_sets', 'review_votes'
          for_table :custom_src_query, :copy_columns => ['id', 'foo']
        end
      end
      raise RuntimeError, "Shouldn't be here"
    rescue ArgumentError
    end
  end

  it "should throw an ArgumentError if the columns specified in the for_table method does not exist" do
    begin
      app = DbCopier.app do
        copy :from => source_db,
             :to => mock_db, :mock_copy => true do
          except 'custom_src_query_ratings', 'result_sets', 'review_votes'
          for_table :custom_src_query_ratings, :copy_columns => ['id', 'foo']
        end
      end
      raise RuntimeError, "Shouldn't be here"
    rescue ArgumentError
    end
  end

  it "should copy only columns that are there in the target db" do
    begin
      db_target = Sequel.connect mock_db
      db_target.create_table :users do
        primary_key :id
        String :name
      end
      app = DbCopier.app do
        copy :from => source_db, :to => mock_db do
          only 'users'
        end
      end
      db_src = Sequel.connect source_db
      db_src[:users].count.should == db_target[:users].count
    ensure
      db_target.drop_table(:users) if db_target && db_target.table_exists?(:users)
    end
  end

  it "should handle blobs"
  it "should create tables in the target db if they do not exist"


end
