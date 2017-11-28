require 'nokogiri'
require 'pp'
require 'yaml'
require 'pathname'

koha_root = Pathname.new(ARGV[0])
debug = false
marc_flavours = ['marc21', 'unimarc', 'nomarc']

indexes = {
  'authorities' => {
    'indexdefs' => 'authority-koha-indexdefs.xml',
  },
  'biblios' => {
    'indexdefs' => 'biblio-koha-indexdefs.xml',
  }
}

format_range_target = ->(field, offset, length) {
  offset = Integer(offset)
  length = Integer(length)
  range = nil
  if length > 1
    range = "#{offset}-#{offset - 1 + length}"
  elsif length == 1
    range = "#{offset}"
  elsif length == 0
    # WTF offset 39 length 0
    range = "#{offset}"
  end
  "#{field}_/#{range}"
}

elastic_mappings = YAML::load_file(koha_root.join('admin/searchengine/elasticsearch/mappings.yaml'));
new_elastic_mappings = {
  'authorities' => {},
  'biblios' => {}
}

indexes.each do |index_name, index_info|
  marc_flavours.each do |marc_flavour|
    marc_dir = koha_root.join('etc/zebradb/marc_defs').join(marc_flavour)
    next unless marc_dir.exist?

    mappings = Hash.new { |hash, key| hash[key] = [] }
    assign_target_to_fields = ->(marc_target_elem, marc_target) {
      marc_target_elem.xpath(".//target_index")
        .map { |index| index.content.split(':')[0] }
        .uniq
        .each do |field|
        mappings[field] << marc_target
      end
      #marc_target_elem.xpath(".//target_index").each do |index|
      #  field = index.content.split(':')[0]
      #  mappings[field] << marc_target
      #end
    }

    handle_marc_target_elem = ->(marc_target_name, elem) {
      if elem.attribute('offset')
        marc_target_name = format_range_target.call(marc_target_name, elem.attribute('offset').value, elem.attribute('length').value)
      end
      assign_target_to_fields.call(elem, marc_target_name)
    }

    doc = File.open(marc_dir.join(index_name).join(index_info['indexdefs'])) { |f| Nokogiri::XML(f) }
    doc.remove_namespaces!

    doc.xpath("//index_leader").each do |leader|
      handle_marc_target_elem.call('leader', leader);
    end

    doc.xpath("//index_control_field").each do |field|
      target = field.attribute('tag').value
      handle_marc_target_elem.call(target, field);
    end

    doc.xpath("//index_data_field").each do |field|
      target = field.attribute('tag').value
      handle_marc_target_elem.call(target, field);
    end

    doc.xpath("//index_subfields").each do |field|
      target = field.attribute('tag').value + field.attribute('subfields')
      handle_marc_target_elem.call(target, field);
    end

    # Normalize targets
    subfields_regexp = Regexp.compile('^([0-9]{3})([0-9a-z]+)$')

    # Group subfields by field
    mappings = mappings.map do |field, targets|
      h = Hash.new { |hash, key| hash[key] = '' }
      grouped = targets.reduce(h) do |grouped_targets, target|
        matches = subfields_regexp.match(target)
        if not matches.nil?
          # Insert alphabetically
          grouped_targets[matches[1]] =
            grouped_targets[matches[1]].chars.push(matches[2]).sort.join
        else
          grouped_targets[target] = ''
        end
        grouped_targets
      end
      [field, grouped]
    end.to_h

    #datafield_regexp = Regexp.compile('^(\d[1-9])|([1-9]\d)\d{2}$')
    # Restore subfield names
    mappings = mappings.map do |field, targets|
      #default = 'abcdefghklmnoprstvxyz'
      #[field, targets.map { |target, subfields| target + (subfields.empty? && datafield_regexp.match(target) ? default : subfields) }]
      [field, targets.map { |target, subfields| target + subfields }]
    end.to_h

    if debug
      h = Hash.new { |hash, key| hash[key] = [] }
      duplicates = mappings.reduce(h) do |grouped_by_target, (field, targets)|
        target_key = targets.sort.join
        grouped_by_target[target_key] << field
        grouped_by_target
      end.select { |_, fields| fields.length > 1 }
      puts "# Duplicates:"
      pp duplicates
    end

    # Convert into Koha elasticsearch mappings.yaml format
    mappings.each do |field, marc_targets|
      m = marc_targets.map do |target|
        {
          'facet' => '',
          'marc_field' => target,
          'marc_type' => marc_flavour, #@TODO
          'sort' => nil, #@TODO
          'suggestible' => ''
        }
      end
      new_elastic_mappings[field] = {
        'label' => field,
        'mappings' => m,
        'type' => '' #TODO: How handle this, set manually?
      }
    end

    # Compare mappings for intersected fields
    if debug
      puts "### Different mapping:"
      elastic_mappings['biblios'].each do |field, opts|
        if mappings[field]
          elastic_targets = opts['mappings']
            .select { |mapping| mapping['marc_type'] == marc_flavour }
            .map { |mapping| mapping['marc_field'] }
            .sort
            .join("\n")
          zebra_targets = mappings[field].sort.join("\n")
          if elastic_targets != zebra_targets
            puts "Field: #{field}"
            puts "mappings.yaml:\n"
            puts elastic_targets
            puts "Zebra:\n"
            puts zebra_targets
            puts "-------------"
          end
        end
      end
    end

    # TODO: rename new_elastic_mappings
    # Overlay missing mappings and sort alphabetically
    unless debug
      index_mappings = {}
      new_elastic_mappings[index_name].keys.sort_by { |f| f.downcase }.each do |field|
        if elastic_mappings[index_name][field]
          index_mappings[field] = elastic_mappings[index_name][field]
        else
          index_mappings[field] = new_elastic_mappings[index_name][field]
        end
      end
      new_elastic_mappings[index_name] = index_mappings
    end

    if debug
      zebra_lwrcase = mappings.keys.map(&:downcase)
      elastic_lwrcase = elastic_mappings[index_name].keys.map(&:downcase)
      case_diff = {}

      puts "## In Zebra but not in Elastic:\n" if debug
      mappings.keys.each do |key|
        if not elastic_mappings[index_name].has_key?(key)
          if elastic_lwrcase.include?(key.downcase)
            case_diff[key.downcase] = {'zebra' => key}
          else
            puts "'#{key}'" if debug
          end
        end
      end

      puts "## In Elastic but not in Zebra:\n"
      new_elastic_mappings[index_name].keys.each do |key|
        if not mappings.has_key?(key)
          if zebra_lwrcase.include?(key.downcase)
            case_diff[key.downcase]['elastic'] = key
          else
            puts "'#{key}'"
          end
        end
      end
      puts "## Different case: \n"
      puts case_diff.to_yaml
    end
  end
end
unless debug
  yaml = new_elastic_mappings.to_yaml
  # Fixing koha-yaml idiosyncrasies in a horrible way
  # Fix sequence indentation:
  yaml.gsub!(/- facet:| marc_field:| marc_type:| sort:| suggestible:/, '  \\0')
  # Fix undef = ~:
  yaml.gsub!(/ sort: /, ' sort: ~')
  puts yaml
end
# TODO: Handle facets!
