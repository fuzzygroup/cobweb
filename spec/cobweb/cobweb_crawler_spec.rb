require File.expand_path(File.dirname(__FILE__) + '/../spec_helper')

describe CobwebCrawler do

  before(:each) do
    pending("thin not installed") unless THIN_INSTALLED

    @base_url = "http://localhost:3532/"

    @default_headers = {"Cache-Control" => "private, max-age=0",
                        "Date" => "Wed, 10 Nov 2010 09:06:17 GMT",
                        "Expires" => "-1",
                        "Content-Type" => "text/html; charset=UTF-8",
                        "Content-Encoding" => "gzip",
                        "Transfer-Encoding" => "chunked",
                        "Server" => "gws",
                        "X-XSS-Protection" => "1; mode=block"}

  end


  it "should generate a cobweb_crawler object" do
    CobwebCrawler.new.should be_an_instance_of CobwebCrawler
  end

  describe "crawl" do

    it "should crawl a site" do

      @crawler = CobwebCrawler.new({:cache => false, :quiet => true, :debug => false})
      @statistics = @crawler.crawl(@base_url)

      @statistics.should_not be_nil
      @statistics.get_statistics.should be_an_instance_of Hash

      @statistics.get_statistics[:mime_counts]["text/html"].should == 8
      @statistics.get_statistics[:mime_counts]["text/css"].should == 18
      @statistics.get_statistics[:mime_counts]["image/jpeg"].should == 25

    end

    it "should take a block" do
      crawler = CobwebCrawler.new({:cache => false, :quiet => true, :debug => false, :crawl_limit => 1})
      statistics = crawler.crawl(@base_url) do |content, statistics|
        content[:url].should_not be_nil
        statistics[:average_length].should_not be_nil
      end

      statistics.should_not be_nil
      statistics.get_statistics.should be_an_instance_of Hash

      statistics.get_statistics[:mime_counts]["text/html"].should == 1

    end

    context "internal_links" do
      it "should match internal links without being explicitly set" do
        crawler = CobwebCrawler.new({:cache => false, :crawl_limit => 1})
        crawler.crawl(@base_url)
        queued_links = @redis_mock_object.smembers("queued")
        queued_links.should_not include("http://themeforest.net/item/cleandream/490140")
        queued_links.should include("http://localhost:3532/secure")
      end
      context "with https" do
        it "should match https by default" do
          crawler = CobwebCrawler.new({:cache => false, :crawl_limit => 1})
          crawler.crawl(@base_url)
          queued_links = @redis_mock_object.smembers("queued")
          queued_links.should_not include("https://localhost:3532/secure")
          queued_links.should include("http://localhost:3532/secure")
        end
        it "should not define https as different if treat_https_as_http is true" do
          crawler = CobwebCrawler.new({:cache => false, :crawl_limit => 1, :treat_https_as_http => true})
          crawler.crawl(@base_url)
          queued_links = @redis_mock_object.smembers("queued")
          queued_links.should_not include("https://localhost:3532/secure")
          queued_links.should include("http://localhost:3532/secure")
        end
        it "should define https as different if treat_https_as_http is false" do
          crawler = CobwebCrawler.new({:cache => false, :crawl_limit => 1, :treat_https_as_http => false})
          crawler.crawl(@base_url)
          queued_links = @redis_mock_object.smembers("queued")
          queued_links.should_not include("https://localhost:3532/secure")
          queued_links.should_not include("http://localhost:3532/secure")
        end
      end
    end

    context "storing inbound links" do

      before(:each) do
        pending("thin not installed") unless THIN_INSTALLED
        @crawler = CobwebCrawler.new({:cache => false, :quiet => true, :debug => false, :store_inbound_links => true})
        @statistics = @crawler.crawl(@base_url)
      end

      it "should store inbound links" do
        @statistics.inbound_links_for("http://localhost:3532/typography.html").should_not be_empty
        @statistics.inbound_links_for("http://localhost:3532/typography.html").sort.should == ["http://localhost:3532/gallery.html", "http://localhost:3532/boxgridwithsillyname.html", "http://localhost:3532/more.html", "http://localhost:3532/", "http://localhost:3532/tables.html", "http://localhost:3532/typography.html", "http://localhost:3532/forms.html", "http://localhost:3532/dashboard.html"].sort
      end

      it "should handle url encoding" do
        @statistics.inbound_links_for("http://localhost:3532/boxgridwithsillyname.html").sort.should == ["http://localhost:3532/boxgridwithsillyname.html", "http://localhost:3532/gallery.html", "http://localhost:3532/more.html", "http://localhost:3532/tables.html", "http://localhost:3532/typography.html", "http://localhost:3532/forms.html", "http://localhost:3532/dashboard.html"].sort
      end

    end
  end

end
