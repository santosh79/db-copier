require File.dirname(__FILE__) + "/spec_helper.rb"

describe DbCopier do
  #Proc to truncate tables
  clean_up = lambda {|db_info, table_to_truncate| Sequel.connect(db_info)[table_to_truncate.to_sym].truncate }

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
        copy :from => {:adapter => 'mysql', :host => 'localhost', :user => 'rootsss', :password => '', :database => 'db_copier_test'}, :to => {:adapter => 'mysql', :host => 'localhost', :user => 'rootsss', :password => '', :database => 'db_copier_test'}
      end
      raise RuntimeError, "shouldn't be here"
    rescue Sequel::DatabaseConnectionError
    end
  end

  it "should copy all tables if no #table methods are called in the dsl" do
    app = DbCopier.app do
      copy :from => {:adapter => 'mysql', :host => 'localhost', :user => 'root', :password => '', :database => 'dharma_development'},
        :to => {:adapter => 'mysql', :host => 'localhost', :user => 'root', :password => '', :database => 'db_copier_test'}, :mock_copy => true
    end
    ['clicks','custom_src_query_ratings','invitations','result_sets','review_votes','schema_migrations','search_reviews','shares','source_ratings','station_searches','station_sources','stations','suggested_sources','twitter_oauth_infos','user_custom_src_query_ratings','user_source_query_histories','users','users_followers','votes'].each do |tab|
      app.tables_to_copy.should include(tab.to_sym)
    end
  end

  it "should copy only select tables if they are provided as an #only method in the dsl" do
    app = DbCopier.app do
      copy :from => {:adapter => 'mysql', :host => 'localhost', :user => 'root', :password => '', :database => 'dharma_development'}, :to => {:adapter => 'mysql', :host => 'localhost', :user => 'root', :password => '', :database => 'db_copier_test'}, :mock_copy => true do
        only 'custom_src_query_ratings', 'result_sets', 'review_votes'
      end
    end
    app.tables_to_copy.map {|tab| tab.to_s}.sort.should == ['custom_src_query_ratings', 'result_sets', 'review_votes']
  end



  it "should copy only select tables if they are provided as an #only method in the dsl - real test" do
    target_db = {:adapter => 'mysql', :host => 'localhost', :user => 'root', :password => '', :database => 'admin_tools_development'}
    app = DbCopier.app do
      copy :from => {:adapter => 'mysql', :host => 'localhost', :user => 'root', :password => '', :database => 'dharma_development'},
        :to => target_db,
        :rows_per_copy => 1000 do
          only 'custom_src_query_ratings'
        end
    end
    app.tables_to_copy.map {|tab| tab.to_s}.sort.should == ['custom_src_query_ratings']
    clean_up.call(target_db,'custom_src_query_ratings')
  end

  it "should not copy tables specified in the #except dsl method" do
    app = DbCopier.app do
      copy :from => {:adapter => 'mysql', :host => 'localhost', :user => 'root', :password => '', :database => 'dharma_development'}, :to => {:adapter => 'mysql', :host => 'localhost', :user => 'root', :password => '', :database => 'db_copier_test'}, :mock_copy => true do
        except 'custom_src_query_ratings', 'result_sets', 'review_votes'
      end
    end
    app.tables_to_copy.map {|tab| tab.to_s}.sort.should == ['clicks','invitations','schema_migrations','search_reviews','shares','source_ratings','station_searches','station_sources','stations','suggested_sources','twitter_oauth_infos','user_custom_src_query_ratings','user_source_query_histories','users','users_followers','votes']
  end



end
