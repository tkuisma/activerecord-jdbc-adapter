module ::ArJdbc
  module HSQLDB
    def self.column_selector
      [/hsqldb|\.h2\./i, lambda {|cfg,col| col.extend(::ArJdbc::HSQLDB::Column)}]
    end

    module Column
      private
      def simplified_type(field_type)
        case field_type
        when /longvarchar/i then :text
        when /tinyint/i  then :boolean
        when /real/i     then :float
        else
          super
        end
      end

      # Override of ActiveRecord::ConnectionAdapters::Column
      def extract_limit(sql_type)
        # HSQLDB appears to return "LONGVARCHAR(0)" for :text columns, which
        # for AR purposes should be interpreted as "no limit"
        return nil if sql_type =~ /\(0\)/
        super
      end

      # Post process default value from JDBC into a Rails-friendly format (columns{-internal})
      def default_value(value)
        # H2 auto-generated key default value
        return nil if value =~ /^\(NEXT VALUE FOR/i

        # jdbc returns column default strings with actual single quotes around the value.
        return $1 if value =~ /^'(.*)'$/

        value
      end
    end

    def adapter_name #:nodoc:
      'Hsqldb'
    end

    def self.arel2_visitors(config)
      require 'arel/visitors/hsqldb'
      {}.tap {|v| %w(hsqldb jdbchsqldb).each {|a| v[a] = ::Arel::Visitors::HSQLDB } }
    end

    def modify_types(tp)
      tp[:primary_key] = "INTEGER GENERATED BY DEFAULT AS IDENTITY(START WITH 0) PRIMARY KEY"
      tp[:integer][:limit] = nil
      tp[:boolean][:limit] = nil
      # set text and float limits so we don't see odd scales tacked on
      # in migrations
      tp[:boolean] = { :name => "tinyint" }
      tp[:text][:limit] = nil
      tp[:float][:limit] = 17 if defined?(::Jdbc::H2)
      tp[:string][:limit] = 255
      tp[:datetime] = { :name => "DATETIME" }
      tp[:timestamp] = { :name => "DATETIME" }
      tp[:time] = { :name => "TIME" }
      tp[:date] = { :name => "DATE" }
      tp
    end

    def quote(value, column = nil) # :nodoc:
      return value.quoted_id if value.respond_to?(:quoted_id)

      case value
      when String
        if respond_to?(:h2_adapter) && value.empty?
          "''"
        elsif column && column.type == :binary
          "X'#{value.unpack("H*")[0]}'"
        elsif column && (column.type == :integer ||
                         column.respond_to?(:primary) && column.primary && column.klass != String)
          value.to_i.to_s
        else
          "'#{quote_string(value)}'"
        end
      else
        super
      end
    end

    def quote_column_name(name) #:nodoc:
      name = name.to_s
      if name =~ /[-]/
        %Q{"#{name.upcase}"}
      else
        name
      end
    end

    def quote_string(str)
      str.gsub(/'/, "''")
    end

    def quoted_true
      '1'
    end

    def quoted_false
      '0'
    end

    def add_column(table_name, column_name, type, options = {})
      add_column_sql = "ALTER TABLE #{quote_table_name(table_name)} ADD #{quote_column_name(column_name)} #{type_to_sql(type, options[:limit], options[:precision], options[:scale])}"
      add_column_options!(add_column_sql, options)
      execute(add_column_sql)
    end

    def change_column(table_name, column_name, type, options = {}) #:nodoc:
      execute "ALTER TABLE #{table_name} ALTER COLUMN #{column_name} #{type_to_sql(type, options[:limit])}"
    end

    def change_column_default(table_name, column_name, default) #:nodoc:
      execute "ALTER TABLE #{table_name} ALTER COLUMN #{column_name} SET DEFAULT #{quote(default)}"
    end

    def rename_column(table_name, column_name, new_column_name) #:nodoc:
      execute "ALTER TABLE #{table_name} ALTER COLUMN #{column_name} RENAME TO #{new_column_name}"
    end

    # Maps logical Rails types to MySQL-specific data types.
    def type_to_sql(type, limit = nil, precision = nil, scale = nil)
      return super if defined?(::Jdbc::H2) || type.to_s != 'integer' || limit == nil

      type
    end

    def rename_table(name, new_name)
      execute "ALTER TABLE #{name} RENAME TO #{new_name}"
    end

    def last_insert_id
      identity = select_value("CALL IDENTITY()")
      Integer(identity.nil? ? 0 : identity)
    end

    def _execute(sql, name = nil)
      result = super
      ActiveRecord::ConnectionAdapters::JdbcConnection::insert?(sql) ? last_insert_id : result
    end

    def add_limit_offset!(sql, options) #:nodoc:
      if sql =~ /^select/i
        offset = options[:offset] || 0
        bef = sql[7..-1]
        if limit = options[:limit]
          sql.replace "SELECT LIMIT #{offset} #{limit} #{bef}"
        elsif offset > 0
          sql.replace "SELECT LIMIT #{offset} 0 #{bef}"
        end
      end
    end

    # override to filter out system tables that otherwise end
    # up in db/schema.rb during migrations.  JdbcConnection#tables
    # now takes an optional block filter so we can screen out
    # rows corresponding to system tables.  HSQLDB names its
    # system tables SYSTEM.*, but H2 seems to name them without
    # any kind of convention
    def tables
      @connection.tables.select {|row| row.to_s !~ /^system_/i }
    end

    def remove_index(table_name, options = {})
      execute "DROP INDEX #{quote_column_name(index_name(table_name, options))}"
    end

    def recreate_database(name)
      drop_database(name)
    end
    
    # do nothing since database gets created upon connection. However
    # this method gets called by rails db rake tasks so now we're
    # avoiding method_missing error
    def create_database(name)
    end

    def drop_database(name)
      execute("DROP ALL OBJECTS")
    end    
  end
end
