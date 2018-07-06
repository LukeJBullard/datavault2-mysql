<?php
    /**
     * DataVault 2.0 module for OliveWeb
     * A Link Table backed by MySQL for Storage
     * 
     * @author Luke Bullard
     */

    /**
     * A LinkTable that saves to an MySQL Table
     */
    class MySQLLinkTable extends LinkTable
    {
        protected $m_tableName;
        protected $m_sourceFieldName;
        protected $m_hashKeyFieldName;
        protected $m_loadDateFieldName;
        protected $m_fieldMap;
        protected $m_connectionName;
        protected $m_pdo;

        /**
         * Constructor for MySQLLinkTable
         * 
         * @param String $a_connectionName The name of the Olive PDO connection to use when communicating with the MySQL database
         * @param String $a_tableName The name of the table to store the Links in
         * @param String $a_sourceFieldName The name of the column that stores the initial source of the Link
         * @param String $a_loadDateFieldName The name of the column that stores the initial load date of the Link
         * @param String $a_hashKeyFieldName The name of the column that stores the hash of the Link (Primary key)
         * @param Array $a_fieldMap An associative array. Each Key is the name of the linked hub, and the Value is the MySQL table column
         *              to put that hub's hash in
         */
        public function __construct($a_connectionName, $a_tableName, $a_sourceFieldName, $a_loadDateFieldName, $a_hashKeyFieldName, $a_fieldMap)
        {
            $this->m_tableName = $a_tableName;
            $this->m_sourceFieldName = $a_sourceFieldName;
            $this->m_loadDateFieldName = $a_loadDateFieldName;
            $this->m_hashKeyFieldName = $a_hashKeyFieldName;

            //lowercase the fields in the fieldmap, but not the column names
            $this->m_fieldMap = array_change_key_case($a_fieldMap);

            $this->m_connectionName = $a_connectionName;

            $this->m_pdo = Modules::getInstance()['pdo'];
        }

        /**
         * Retrieves if a link exists in the Table
         * 
         * @param String $a_hash The hash of the link to search for
         * @return Boolean If the link exists
         */
        public function linkExists($a_hash)
        {
            $query = "SELECT ? FROM ? WHERE ?=? LIMIT 1";

            $this->m_pdo->chooseConnection($this->m_connectionName);
            $result = $this->m_pdo->select($query,
                $this->m_hashKeyFieldName,
                $this->m_tableName,
                $this->m_hashKeyFieldName, $a_hash
            );

            if (is_array($result) && !empty($result))
            {
                return true;
            }

            return false;
        }

        /**
         * Deletes a Link from the table
         * 
         * @param String $a_hash The Hash of the Link to delete
         * @return Int DV2_SUCCESS or DV2_ERROR
         */
        public function clearLink($a_hash)
        {
            $query = "DELETE FROM ? WHERE ?=?";
            $result = $this->m_pdo->execute($query,
                $this->m_tableName,
                $this->m_hashKeyFieldName, $a_hash
            );
            
            //error returns negative int
            if (is_int($result) && $result < 0)
            {
                return DV2_ERROR;
            }

            return DV2_SUCCESS;
        }

        /**
         * Saves a Link to the table
         * 
         * @param Link $a_link The link to save
         * @return Int DV2_SUCCESS or DV2_ERROR
         */
        public function saveLink($a_link)
        {
            $query = "INSERT INTO ? (";
            $questionMarks = "?,?,?";
            $columnArgs = array($this->m_hashKeyFieldName, $this->m_loadDateFieldName, $this->m_sourceFieldName);
            $valueArgs = array($a_link->getHashKey(), date("Y-m-d"), $a_link->getSource());

            //get links and set all field keys to lowercase
            $links = array_change_key_case($a_link->getLinks());

            //loop through all possible fields
            foreach ($this->m_fieldMap as $field => $column)
            {
                //if the link has the field set
                if (isset($links[$field]))
                {
                    //add the question mark to the query
                    $questionMarks .= ",?";

                    //add the column to the query
                    array_push($columnArgs, $column);

                    //add the value to the query
                    array_push($valueArgs, $links[$field]);
                }
            }

            $query .= $questionMarks . ") SELECT * FROM (SELECT " . $questionMarks . ") AS tmp ";
            $query .= "WHERE NOT EXISTS (SELECT 1 FROM ? WHERE ?=?) LIMIT 1";

            //merge the column and value args, preparing to call pdo select
            $args = array_merge($columnArgs, $valueArgs);
            
            //add the table name and query to the beginning of the argument list
            array_unshift($args, $query, $this->m_tableName);

            //add the where not exists fields and variables to the argument list
            array_push($args, $this->m_tableName, $this->m_hashKeyFieldName, $a_link->getHashKey());

            $this->m_pdo->chooseConnection($this->m_connectionName);
            $result = call_user_func_array(array($this->m_pdo, "execute"), $args);

            //if the result is a negative int, an error occurred
            if (is_int($result) && $result < 0)
            {
                return DV2_ERROR;
            }

            return DV2_SUCCESS;
        }

        /**
         * Retrieves a Link from the MySQL Table
         * 
         * @param String $a_hash The Hash of the Link to retrieve
         * @return Link|Int The returned Link or the Int DV2 status code if the Link could not be retrieved
         */
        public function getLink($a_hash)
        {
            $query = "SELECT ";
            $columns = "?,?,?";
            $args = array($this->m_sourceFieldName, $this->m_loadDateFieldName, $this->m_hashKeyFieldName);
            
            foreach ($this->m_fieldMap as $field => $column)
            {
                $columns .= ",?";
                array_push($args, $column);
            }

            $query .= $columns . " FROM ? WHERE ?=?";
            array_push($args, $this->m_tableName, $this->m_hashKeyFieldName, $a_hash);
            array_unshift($args, $query);

            $this->m_pdo->chooseConnection($this->m_connectionName);
            $result = call_user_func_array(array($this->m_pdo, "select"), $args);

            //if result is negative int, an error occurred
            if (is_int($result) && $result < 0)
            {
                return DV2_ERROR;
            }

            //if the link was found
            if (is_array($result) && !empty($result))
            {
                $links = array();
                $source = "";
                $date = "";

                foreach ($result[0] as $fieldName => $val)
                {
                    switch ($fieldName)
                    {
                        case $this->m_sourceFieldName:
                            $source = $val;
                            continue;
                        
                        case $this->m_dateFieldName:
                            $date = $val;
                            continue;
                    }

                    //find the key name for the fieldname in the fieldmap
                    $keyName = array_search($fieldName, $this->m_fieldMap);

                    //if no key was found
                    if (!is_string($keyName))
                    {
                        continue;
                    }

                    $links[$keyName] = $val;
                }

                //if the link is invalid, return error
                if ($source == "" || $date == "" || empty($links))
                {
                    return DV2_ERROR;
                }

                return new Link(
                    $source,
                    $date,
                    $links
                );
            }

            //link not found, return error
            return DV2_ERROR;
        }
    }
?>