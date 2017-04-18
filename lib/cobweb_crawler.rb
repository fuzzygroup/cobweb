require 'digest/md5'
require 'date'
require 'redis-namespace'

# CobwebCrawler is a standalone crawler, it includes a built in statistics monitor using Sinatra.
class CobwebCrawler
  
  # See README for more information on options available
  def initialize(options={})
    @options = options
    
    @statistic = {}
    
    @options[:redis_options] = {:host => "127.0.0.1"} unless @options.has_key? :redis_options
    if @options.has_key? :crawl_id
      @crawl_id = @options[:crawl_id]
    else
      @crawl_id = Digest::MD5.hexdigest(DateTime.now.inspect.to_s)
      @options[:crawl_id] = @crawl_id
    end
    
    @redis = Redis::Namespace.new("cobweb-#{Cobweb.version}-#{@crawl_id}", :redis => RedisConnection.new(@options[:redis_options]))
    @options[:internal_urls] = [] if @options[:internal_urls].nil?
    @options[:internal_urls].map{|url| @redis.sadd("internal_urls", url)}
    @options[:seed_urls] = [] if @options[:seed_urls].nil?
    @options[:seed_urls].map{|link| @redis.sadd "queued", link }

    @options[:crawl_linked_external] = false unless @options.has_key? :crawl_linked_external

    @options[:treat_https_as_http] = true unless @options.has_key? :treat_https_as_http     
    @debug = @options[:debug]
    
    @stats = Stats.new(@options.merge(:crawl_id => @crawl_id))
    if @options[:web_statistics]
      Server.start(@options)
    end
    
    @cobweb = Cobweb.new(@options)
  end
  
  # Initiates a crawl starting at the base_url and applying the options supplied. Can also take a block that is executed and passed content hash and statistic hash'
  def crawl(base_url, crawl_options = {}, &block)
    @options[:base_url] = base_url unless @options.has_key? :base_url
    @options[:thread_count] = 1 unless @options.has_key? :thread_count
    
    @options[:internal_urls] << base_url if @options[:internal_urls].empty?
    @redis.sadd("internal_urls", base_url) if @options[:internal_urls].empty?
    
    @crawl_options = crawl_options
    
    @redis.sadd("queued", base_url) unless base_url.nil? || @redis.sismember("crawled", base_url) || @redis.sismember("queued", base_url)
    @crawl_counter = @redis.scard("crawled").to_i
    @queue_counter = @redis.scard("queued").to_i

    @threads = []
    begin
      @stats.start_crawl(@options)
      
      @threads << Thread.new do
        Thread.abort_on_exception = true
        spawn_thread(&block)
      end

      sleep 5
      while running_thread_count > 0
        if @queue_counter > 0
          (@options[:thread_count]-running_thread_count).times.each do
            @threads << Thread.new do
              Thread.abort_on_exception = true
              spawn_thread(&block)
            end
          end
        end
        sleep 1
      end
      
    ensure
      @stats.end_crawl(@options)
    end
    @stats
  end
  
  def is_duplicate_content?(content)
    #
    # Approach is as follows:
    #  * check for duplication by SHA1 calculation at full html page level
    #  * segment the full html page into just content between body and close body
    #  * check for duplication by SHA1 calculation at html body content level
    #  * check for duplication at url level by stripping URL next content like:
    #      https://www.udemy.com/join/signup-popup/?next=/gift/ebaytips/
    #    content[:base_url]
    
    #require ''
    #debugger
    
    #
    # Abort duplicate detection unless content[:mime_type] is text/html
    #
    return if content[:mime_type] != "text/html"

    #
    # Check 1 -- full html 
    #
    sha1 = Digest::SHA1.hexdigest(content[:body])
    if @redis.sismember("duplicate_content", sha1) 
      puts "************************************************************"
      puts "Got into duplicate content check AT html level and already exists in there so returning true"
      puts "************************************************************"
      return true 
    end
    
    #
    # Check 2 -- content inside body
    #
    mechanize_page = Mechanize::Page.new(nil, {'content-type'=>'text/html'}, content[:body], nil, Mechanize.new)
    mechanize_page.uri = URI.parse(content[:url])    
    content_inside_body = mechanize_page.parser.css('body').to_html.sub(/<body[^>]*>/i,'').sub(/<\/body>/i,'')
    sha1_body = Digest::SHA1.hexdigest(content_inside_body)
    if @redis.sismember("duplicate_content", sha1_body)
      puts "************************************************************"
      puts "Got into duplicate content check AT body level and already exists in there so returning true"
      puts "************************************************************"
      return true       
    end
    
    #
    # Check 3 -- strip off fragment content -- 
    #   really needs to be configurable -- not just hard coded like this
    #
    if content[:url] =~ /\?next/ || content[:url] =~ /\?key/
      parts = URI.parse(content[:url])
      cleaned_url = "parts.scheme://#{parts.hostname}#{parts.path}"
      sha1_url = Digest::SHA1.hexdigest(cleaned_url)
      if @redis.sismember("duplicate_content", sha1_url)
        puts "************************************************************"
        puts "Got into duplicate content check AT url level and already exists in there so returning true"
        puts "************************************************************"
        return true       
      end      
    end
    
    #
    # Didn't pass the checks so adding sha's
    #
    puts "************************************************************"
    puts "Not duplicate content so adding 3 shas and returning false"
    puts "************************************************************"
    @redis.sadd("duplicate_content", sha1)
    @redis.sadd("duplicate_content", sha1_body)
    @redis.sadd("duplicate_content", sha1_url)
    return false
  end
  
  #Online Courses - Anytime, Anywhere | Udemy
  def is_duplicate_title(content)
  end  

  def spawn_thread(&block)
      while @queue_counter>0 && (@options[:crawl_limit].to_i == 0 || @options[:crawl_limit].to_i > @crawl_counter)
        url = @redis.spop "queued"
      @queue_counter = 0 if url.nil?

      @options[:url] = url
      unless @redis.sismember("crawled", url.to_s)
        begin
          @stats.update_status("Requesting #{url}...")
          content = @cobweb.get(url) unless url.nil?
          #debugger
          if content.nil? || is_duplicate_content?(content)
            @queue_counter = @queue_counter - 1 #@redis.scard("queued").to_i
          else
            @stats.update_status("Processing #{url}...")

            @redis.sadd "crawled", url.to_s
            @redis.incr "crawl-counter" 
          
            document_links = ContentLinkParser.new(url, content[:body]).all_links(:valid_schemes => [:http, :https]).uniq


            # select the link if its internal (eliminate external before expensive lookups in queued and crawled)
            cobweb_links = CobwebLinks.new(@options)

            internal_links = document_links.select{|link| cobweb_links.internal?(link) || (@options[:crawl_linked_external] && cobweb_links.internal?(url.to_s) && !cobweb_links.matches_external?(link))}

            # if the site has the same content for http and https then normalize to http 
            
            if @options[:treat_https_as_http]
              internal_links.map!{|link| link.gsub(/^https/, "http")}
            end
            

            # reject the link if we've crawled it or queued it
            internal_links.reject!{|link| @redis.sismember("crawled", link)}
            internal_links.reject!{|link| @redis.sismember("queued", link)}
            internal_links.reject!{|link| link.nil? || link.empty?}
          
            internal_links.each do |link|
              puts "Added #{link.to_s} to queue" if @debug
              @redis.sadd "queued", link unless link.nil?
              children = @redis.hget("navigation", url)
              children = [] if children.nil?
              children << link
              @redis.hset "navigation", url, children
              @queue_counter += 1
            end

            if @options[:store_inbound_links]
              document_links.each do |target_link|
                target_uri = UriHelper.parse(target_link).normalize
                @redis.sadd("inbound_links_#{Digest::MD5.hexdigest(target_uri.to_s)}", UriHelper.parse(url).to_s)
              end
            end
            
            @crawl_counter = @redis.scard("crawled").to_i
            @queue_counter = @redis.scard("queued").to_i
          
            @stats.update_statistics(content, @crawl_counter, @queue_counter)
            @stats.update_status("Completed #{url}.")
            yield content, @stats.get_statistics if block_given?
          end
        rescue => e
          puts "Error loading #{url}: #{e}"
          #puts "!!!!!!!!!!!! ERROR !!!!!!!!!!!!!!!!"
          #ap e
          #ap e.backtrace
        ensure
          @crawl_counter = @redis.scard("crawled").to_i
          @queue_counter = @redis.scard("queued").to_i
        end
      else
        puts "Already crawled #{@options[:url]}" if @debug
      end
    end
    Thread.exit
  end

  def running_thread_count
    @threads.map{|t| t.status}.select{|status| status=="run" || status == "sleep"}.count
  end
  
end

# Monkey patch into String a starts_with method
class String
  # Monkey patch into String a starts_with method
  def cobweb_starts_with?(val)
    if self.length >= val.length
      self[0..val.length-1] == val
    else
      false
    end
  end
end
