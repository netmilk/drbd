require 'rubygems'
require 'nokogiri'
class DRBD
  attr_reader :resources

  def initialize host
    @host = host
    load!
  end

  def load!
    load_resources!
    load_status!
  end

  def load_resources!
    @resources = Config.new(IO.popen("ssh #{@host} \"sudo /sbin/drbdadm dump-xml\"")).resources
  end

  def load_status!
    raw_xml = IO.popen("ssh #{@host} \"sudo /sbin/drbdadm status\"")
    statuses = Status.new(raw_xml).resources
    set_resources_status statuses
  end

  def set_resources_status statuses 
    statuses.each do |status|
      resource = find_resource_by_name(status[:name])
      resource.status = status
    end
  end

  def find_resource_by_name given_name
    @resources.select{|r| r.name == given_name}.first
  end

  def find_resource_by_disk disk_name
    @resources.select{|r| r.hosts.inject(false){|sum,h| (h.disk == disk_name && sum == false) ? true  : sum}}.first
  end

  class Config
    attr_reader :xml

    def initialize xml
      @xml = Nokogiri::XML(xml)
    end

    def resources
      @xml.xpath("//config/resource").map{|r| Resource.new r }
    end
  end

  class Host
    attr_reader :name, :device, :disk, :address, :meta_disk
    def initialize host
      @name = host['name']
      @device = host.xpath(".//device").text
      @disk = host.xpath(".//disk").text
      @address = host.xpath(".//address").text
      @meta_disk = host.xpath(".//meta-disk").text
    end
  end
  
  class Resource
    attr_reader :name, :device, :disk, :address, :meta_disk, :hosts
    attr_accessor :status
    def initialize nokogiri_resource
      xml = nokogiri_resource
      @name = xml['name']
      @protocol = xml['protocol']
      @hosts = xml.xpath(".//host").to_a.map do |host_xml|
        Host.new host_xml
      end
    end

    def resync_running?
      not status[:resynced_percent] == nil
    end
    
    def consistent?
      status[:ds1] == "UpToDate" && status[:ds2] == "UpToDate" && status[:resynced_percent] == nil
    end
  end

  class Status
    attr_reader :resources
    def initialize raw
      xml = Nokogiri::XML(raw)
      resources = xml.xpath("//drbd-status/resources/resource")
      @resources = resources.map do |resource|
        r = {}
        r[:name] = resource["name"]
        r[:minor] = resource["minor"]
        r[:cs] = resource["cs"]
        r[:ro1] = resource["ro1"]
        r[:ro2] = resource["ro2"]
        r[:ds1] = resource["ds1"]
        r[:ds2] = resource["ds2"]
        r[:resynced_percent] = resource["resynced_percent"]
        r
      end
    end
    
    
  end
end

