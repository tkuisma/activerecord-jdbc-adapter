require 'arjdbc/sybase/limit_helpers'

module ::ArJdbc
  module Sybase
    include LimitHelpers

    def self.arel2_visitors(config)
      require 'arel/visitors/sybase'
      { 'jdbc' => ::Arel::Visitors::Sybase }
    end

    module Column
      attr_accessor :identity

    end

    def add_limit_offset!(sql, options) # :nodoc:
      @limit = options[:limit]
      @offset = options[:offset]
      if use_temp_table?
        # Use temp table to hack offset with Sybase
        sql.sub!(/ FROM /i, ' INTO #artemp FROM ')
      elsif zero_limit?
        # "SET ROWCOUNT 0" turns off limits, so we havesy
        # to use a cheap trick.
        if sql =~ /WHERE/i
          sql.sub!(/WHERE/i, 'WHERE 1 = 2 AND ')
        elsif sql =~ /ORDER\s+BY/i
          sql.sub!(/ORDER\s+BY/i, 'WHERE 1 = 2 ORDER BY')
        else
          sql << 'WHERE 1 = 2'
        end
      end
    end

    # If limit is not set at all, we can ignore offset;
    # if limit *is* set but offset is zero, use normal select
    # with simple SET ROWCOUNT.  Thus, only use the temp table
    # if limit is set and offset > 0.
    def use_temp_table?
      !@limit.nil? && !@offset.nil? && @offset > 0
    end

    def zero_limit?
      !@limit.nil? && @limit == 0
    end

    def modify_types(tp) #:nodoc:
      tp[:primary_key] = "NUMERIC(22,0) IDENTITY PRIMARY KEY"
      tp[:integer][:limit] = nil
      tp[:boolean] = {:name => "bit"}
      tp[:binary] = {:name => "image"}
      tp
    end

    def remove_index(table_name, options = {})
      execute "DROP INDEX #{table_name}.#{index_name(table_name, options)}"
    end

    def determine_order_clause(sql)
      return $1 if sql =~ /ORDER BY (.*)$/
      table_name = get_table_name(sql)
      "#{table_name}.#{determine_primary_key(table_name)}"
    end

    def determine_primary_key(table_name)
      primary_key = columns(table_name).detect { |column| column.primary || column.identity }
      return primary_key.name if primary_key
      # Look for an id column.  Return it, without changing case, to cover dbs with a case-sensitive collation.
      columns(table_name).each { |column| return column.name if column.name =~ /^id$/i }
      # Give up and provide something which is going to crash almost certainly
      columns(table_name)[0].name
    end

  end
end
