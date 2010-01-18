require File.dirname(__FILE__) + "/spec_helper.rb"

describe DbCopier do
  source_db = {:adapter => 'mysql', :host => 'localhost', :user => 'root', :password => '', :database => 'db_copier_test_src'}
  target_db = {:adapter => 'mysql', :host => 'localhost', :user => 'root', :password => '', :database => 'db_copier_test_target'}
  fake_cred_db = target_db.merge(:user => 'rootsss')

  def get_tenor(val)
    if val * 10 <= 3
      "Luciano Pavarotti"
    elsif val * 10 <=6
      "Placido Domingo"
    else
      "Jose Carreras"
    end
  end

  def get_amigo(val)
    if val * 10 <= 3
      "Chevy Chase"
    elsif val * 10 <=6
      "Steve Martin"
    else
      "Martin Short"
    end
  end

  def get_three_tragic_shakesperean_characters(val)
    if val * 10 <= 3
      "King Lear"
    elsif val * 10 <=6
      "Titus Andronicus"
    else
      "Hamlet"
    end
  end

  before(:all) do
    #create some connections
    @source_db_conn, @fake_cred_db_conn, @target_db_conn = Sequel.connect(source_db), Sequel.connect(fake_cred_db),
            Sequel.connect(target_db)
    @source_db_conn.create_table :uno do
      primary_key :id, :integer, :null => false
      String :nombre, :null => true
      DateTime :created_at, :null => false, :default => DateTime.now
    end
    a_thousand_tenors = []
    1000.times { |i| a_thousand_tenors << {:id => (i+1), :nombre => get_tenor(rand), :created_at => DateTime.now}}
    @source_db_conn[:uno].multi_insert(a_thousand_tenors)

    @source_db_conn.create_table :dos do
      primary_key :id, :integer, :null => false
      String :nombre, :null => true
      DateTime :created_at, :null => false, :default => DateTime.now
    end
    a_thousand_amigos = []
    1000.times { |i| a_thousand_amigos<< {:id => (i+1), :nombre => get_amigo(rand), :created_at => DateTime.now}}
    @source_db_conn[:dos].multi_insert(a_thousand_amigos)

    @source_db_conn.create_table :tres do
      primary_key :id, :integer, :null => false
      String :nombre, :null => true
      DateTime :created_at, :null => false, :default => DateTime.now
    end
    a_thousand_tragics = []
    1000.times { |i| a_thousand_amigos<< {:id => (i+1), :nombre => get_three_tragic_shakesperean_characters(rand), :created_at => DateTime.now}}
    @source_db_conn[:dos].multi_insert(a_thousand_tragics)
  end

  after(:each) do
    @target_db_conn.tables.each do |tbl|
      @target_db_conn.drop_table(tbl)
    end
  end

  after(:all) do
    @source_db_conn.drop_table :uno
    @source_db_conn.drop_table :dos
    @source_db_conn.drop_table :tres
  end


  def create_target_tables
    @target_db_conn.create_table :uno do
      primary_key :id, :integer, :null => false
      String :nombre, :null => true
      DateTime :created_at, :null => false
    end
    @target_db_conn.create_table :dos do
      primary_key :id, :integer, :null => false
      String :nombre, :null => true
      DateTime :created_at, :null => false
    end
    @target_db_conn.create_table :tres do
      primary_key :id, :integer, :null => false
      String :nombre, :null => true
      DateTime :created_at, :null => false
    end
  end

  def create_target_with_one_less_column
    @target_db_conn.create_table :uno do
      primary_key :id, :integer, :null => false
      String :nombre, :null => true
    end
  end

  it "should throw an argument error if the +:from+ and +:to+ arguments are not provided for the +copy+ method" do
    create_target_tables
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

  it "should throw an argument error if the +:from+ and +:to+ arguments are NOT hashes" do
    create_target_tables
    begin
      DbCopier.app do
        copy :from => 'foo', :to => 'bar'
      end
      raise RuntimeError, "shouldn't be here"
    rescue ArgumentError
    end
  end

  it "should throw an Sequel::DatabaseConnectionError for invalid credentials" do
    create_target_tables
    begin
      DbCopier.app do
        copy :from => source_db, :to => fake_cred_db
      end
      raise RuntimeError, "shouldn't be here"
    rescue Sequel::DatabaseConnectionError
    end
  end

  it "should copy all tables if no #table methods are called in the dsl" do
    create_target_tables
    app = DbCopier.app do
        copy :from => source_db,
             :to => target_db
      end
      @source_db_conn.tables.map{|tbl| tbl.inspect}.sort.should == @target_db_conn.tables.map{|tbl| tbl.inspect}.sort
  end

  it "should copy only select tables if they are provided as an #only method in the dsl" do
    create_target_tables
    only_tables_to_copy = ['uno', 'dos'].sort
    app = DbCopier.app do
        copy :from => source_db, :to => target_db do
          only *only_tables_to_copy
        end
      end
      only_tables_to_copy.each { |tbl| @source_db_conn[tbl.to_sym].count.should == @target_db_conn[tbl.to_sym].count }
  end

  it "should not copy tables specified in the #except dsl method" do
    create_target_tables
    app = DbCopier.app do
      copy :from => source_db,
           :to => target_db do
        except 'tres'
      end
    end
    app.tables_to_copy.map {|tab| tab.to_s}.sort.should == ['dos', 'uno']
  end

  it "should throw an ArgumentError if the for_table method is called without appropriate args" do
    create_target_tables
    begin
      app = DbCopier.app do
        copy :from => source_db, :to => target_db do
          except 'uno', 'dos'
          for_table :custom_src_query_ratings #Missing :copy_columns argument
        end
      end
      raise RuntimeError, "Shouldn't be here"
    rescue ArgumentError
    end
  end

  it "should only copy the columns specified in the +copy_columns+ argument in the for_table method" do
    create_target_tables
    app = DbCopier.app do
      copy :from => source_db, :to => target_db do
        except 'tres'
        for_table :uno, :copy_columns => ['id', 'created_at']
      end
    end
    @target_db_conn[:uno].all.each do |row|
      row[:nombre].should == nil
    end
  end

  it "should throw an ArgumentError if the columns specified in the for_table method does not exist do not exist in the source table" do
    create_target_tables
    begin
      app = DbCopier.app do
        copy :from => source_db,
             :to => target_db do
          except 'tres'
          for_table :uno, :copy_columns => ['id', 'foo']
        end
      end
      raise RuntimeError, "Shouldn't be here"
    rescue ArgumentError
    end
  end

  it "should copy only columns that are there in the target db" do
    create_target_with_one_less_column
    app = DbCopier.app do
        copy :from => source_db, :to => target_db do
          only 'uno'
        end
    end
    @source_db_conn[:uno].count.should == @target_db_conn[:uno].count
  end

  it "should create tables in the target db if they do not exist" do
    begin
      app = DbCopier.app do
        copy :from => source_db, :to => target_db do
          for_table :uno, :copy_columns => ['id', 'created_at']
        end
      end
      @target_db_conn.tables.map{|tbl| tbl.to_s}.sort.should == @source_db_conn.tables.map{|tbl| tbl.to_s}.sort
      @source_db_conn.tables.each do |tbl|
        @source_db_conn[tbl].count.should == @target_db_conn[tbl].count
      end
    end
  end

  it "should handle blobs"
  it "should work without the mock_copy attribute and without the table_to_copy attribute too"
  it "should figure out how disconnect really works"
  it "should copy views"
  it "should copy indexes"


end
