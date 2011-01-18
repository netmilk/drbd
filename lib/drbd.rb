require 'rubygems'
require 'nokogiri'
class DRBD
  attr_reader :resources, :host, :command

  def initialize host, opts = {}
    parse_opts opts
    @host = host
    load!
  end

  def parse_opts opts
    opts[:command].nil? ? @command = "sudo /sbin/drbdadm" : @command = opts[:command]
  end

  def load!
    load_resources!
    load_status!
  end

  def load_resources!
    @resources = Resource.load_config(IO.popen("ssh #{@host} \"#{@command} dump-xml\""), self)
  end

  def load_status!
    raw_xml = IO.popen("ssh #{@host} \"#{@command} status\"")
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
  
  class Host
    attr_reader :name, :device, :disk, :address, :meta_disk, :minor
    def initialize host
      @name = host['name']
      @device = host.xpath(".//device").text
      @minor = host.xpath(".//device").attr("minor").value
      @disk = host.xpath(".//disk").text
      @address = host.xpath(".//address").text
      @family = host.xpath(".//address").attr("family").value
      @port = host.xpath(".//address").attr("port").value
      @meta_disk = host.xpath(".//meta-disk").text
    end
  end
  
  class Resource
    attr_reader :name, :protocol,  :hosts, :drbd
    attr_accessor :status
    
    def self.load_config raw, drbd
      xml = Nokogiri::XML(raw)    
      xml.xpath("//config/resource").map{|r| Resource.new r, drbd }
    end
    
    def initialize nokogiri_resource, drbd
      xml = nokogiri_resource
      @drbd = drbd
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
    
    def up?
      status[:cs] == "Connected" || status[:cs] == "SyncTarget"
    end
    
    def down?
      status[:cs] == "Unconfigured"
    end 

    def up!
      args = "up #{self.name}"
      command = "ssh #{drbd.host} \"#{drbd.command} #{args}\""
      system(command)
      drbd.load_status!
      nil
    end
    
    def down!
      args = "down #{self.name}"
      command = "ssh #{drbd.host} \"#{drbd.command} #{args}\""
      system(command)
      drbd.load_status!
      nil
    end
    
    def init_metadata!
      if self.down?
        #drbdmeta 0 v08 /dev/mapper/dikobraz-www--emailmaster--cz_root_meta 0 create-md 
        command = "ssh #{drbd.host} \"sudo /sbin/drbdmeta --force #{local_minor} v08 #{local_host.meta_disk} 0 create-md\""
        system(command)
        return true
      else
        return false
      end
    end
    
    
    def local_host
      hosts.select{|h| h.name == drbd.host}.first
    end
     
    def local_minor
      retrurn nil if local_host == nil
      local_host.minor
    end
    
    def state
      status[:cs]
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

