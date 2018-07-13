<?php
    /**
     * Data Vault 2.0 Module for OliveWeb
     * Satellite Table backed by MySQL for Storage
     * 
     * @author Luke Bullard
     */

     /**
     * A SatelliteTable that saves Satellites to a MySQL Table
     */
    class MySQLSatelliteTable extends SatelliteTable
    {
        protected $m_fieldMap;
        protected $m_tableName;
        protected $m_sourceFieldName;
        protected $m_dateFieldName;
        protected $m_hashDiffFieldName;
        protected $m_hubHashFieldName;
        protected $m_connectionName;
        protected $m_pdo;

        /**
         * Constructor for MySQLSatelliteTable
         * 
         * @param String $a_connectionName The name of the Olive PDO connection to use when communicating with the MySQL database
         * @param String $a_tableName The name of the table to use.
         * @param String $a_sourceFieldName The column in the table to put the Satellite's Data Source information.
         * @param String $a_dateFieldName The column in the table to put the Date that the Satellite was loaded.
         * @param String $a_hashDiffFieldName The column in the table to put the Hash of the Satellite.
         * @param String $a_hubHashFieldName The column in the table to put the hash of the Hub the Satellite is linked to.
         * @param Array $a_fieldMap An associative array. Each Key is the Key of the Satellite's Data, and the Value is the
         *              table column it's associated with
         */
        public function __construct($a_connectionName, $a_tableName, $a_sourceFieldName, $a_dateFieldName,
                                    $a_hashDiffFieldName, $a_hubHashFieldName, $a_fieldMap=array())
        {
            $this->m_tableName = $a_tableName;
            $this->m_fieldMap = array_change_key_case($a_fieldMap);
            $this->m_sourceFieldName = $a_sourceFieldName;
            $this->m_dateFieldName = $a_dateFieldName;
            $this->m_hashDiffFieldName = $a_hashDiffFieldName;
            $this->m_hubHashFieldName = $a_hubHashFieldName;
            $this->m_connectionName = $a_connectionName;

            $this->m_pdo = Modules::getInstance()['pdo'];
        }

        /**
         * Returns if the specified Satellite exists in the Table
         * 
         * @param String $a_hashDiff The Hash Diff of the Satellite.
         * @param String $a_hubHash The Hash of the Hub the Satellite is under. (Optional- if omitted, will search for the first Satellite with the hash diff)
         * @return Boolean If the Satellite exists.
         */
        public function satelliteExists($a_hashDiff, $a_hubHash="")
        {
            $this->m_pdo->chooseConnection($this->m_connectionName);

            if ($a_hubHash == "")
            {
                $query = "SELECT 1 FROM `" . $this->m_tableName . "` WHERE `" . $this->m_hashDiffFieldName . "`=? LIMIT 1";
                $result = $this->m_pdo->select($query, $a_hashDiff);
            } else {
                $query = "SELECT 1 FROM `" . $this->m_tableName . "` WHERE `" . $this->m_hashDiffFieldName . "`=? AND `" . $this->m_hubHashFieldName . "`=? LIMIT 1";
                $result = $this->m_pdo->select($query, $a_hashDiff, $a_hubHash);
            }

            return (is_array($result) && !empty($result));
        }

        /**
         * Retrieves a Satellite from the Table
         * @param String $a_hashDiff The Hash Diff of the Satellite to retrieve
         * @param String $a_hubHash The Hash of the Hub the Satellite is under (Optional- if omitted, will return the first Satellite with the hash diff)
         * @return Satellite|Int The Satellite retrieved from the Table or DV2_ERROR If the Satellite was not found or could not be loaded
         */
        public function getSatellite($a_hashDiff, $a_hubHash="")
        {
            $query = "SELECT `" . $this->m_sourceFieldName . "`,`" . $this->m_loadDateFieldName . "`,`" . $this->m_hubHashFieldName . "`";

            //go through each satellite data field and add it to the query
            foreach ($this->m_fieldMap as $field => $column)
            {
                $query .= ",`" . $column . "`";
            }

            $query .= " FROM `" . $this->m_tableName . "` WHERE `" . $this->m_hashDiffFieldName . "`=?";
            $args = array($a_hashDiff);

            //add the hub hash to the where clause if specified
            if ($a_hubHash != "")
            {
                $query .= " AND `" . $this->m_hubHashFieldName . "`=?";
                array_push($args, $a_hubHash);
            }

            $query .= " LIMIT 1";

            $this->m_pdo->chooseConnection($this->m_connectionName);
            $result = call_user_func_array(array($this->m_pdo, "select"), $args);

            //if a row was retrieved
            if (is_array($result) && !empty($result))
            {
                $data = array();
                $source = "";
                $date = "";
                $hubHash = "";

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

                        case $this->m_hubHashFieldName:
                            $hubHash = $val;
                            continue;
                    }

                    //find the key name for the fieldname in the fieldmap
                    $keyName = array_search($fieldName, $this->m_fieldMap);

                    //if no key was found
                    if (!is_string($keyName))
                    {
                        continue;
                    }

                    $data[$keyName] = $val;
                }

                //if the fields are invalid, return error
                if (empty($data) || $source == "" || $date == "" || $hubHash == "")
                {
                    return DV2_ERROR;
                }
                
                return new Satellite(
                    $source,
                    $date,
                    $hubHash,
                    $data
                );
            }

            //either an error occurred or no row was returned
            return DV2_ERROR;
        }

        /**
         * Deletes the Satellite from the Table
         * 
         * @param String $a_hashDiff The hash of the Satellite to delete
         * @param String $a_hubHash The Hash of the Hub the Satellite is under (Optional- if omitted, will clear all Satellites with the hash diff)
         * @return Int DV2_SUCCESS or DV2_ERROR
         */
        public function clearSatellite($a_hashDiff, $a_hubHash="")
        {
            $query = "DELETE FROM `" . $this->m_tableName . "` WHERE `" . $this->m_hashDiffFieldName . "`=?";
            $args = array($a_hashDiff);

            //add the hubhash to the where clause if specified
            if ($a_hubHash != "")
            {
                $query .= " AND `" . $this->m_hubHashFieldName . "`=?";
                array_push($args, $a_hubHash);
            }

            //add the query to the front of the args array, preparing it to be executed
            array_unshift($args, $query);

            $this->m_pdo->chooseConnection($this->m_connectionName);
            $result = call_user_func_array(array($this->m_pdo, "execute"), $args);

            //if negative int, an error occurred
            if (is_int($result) && $result < 0)
            {
                return DV2_ERROR;
            }

            //success!
            return DV2_SUCCESS;
        }

        /**
         * Saves a Satellite in the Table
         * 
         * @param Satellite $a_satellite
         * @return Int DV2_SUCCESS or DV2_ERROR
         */
        public function saveSatellite($a_satellite)
        {
            $query = "INSERT INTO `" . $this->m_tableName . "` (`" . $this->m_hashDiffFieldName . "`,`" . $this->m_hubHashFieldName . "`,`" . $this->m_sourceFieldName . "`,`" . $this->m_dateFieldName . "`";
            $args = array($a_satellite->getHashDiff(), $a_satellite->getHubHash(), $a_satellite->getSource(), date("Y-m-d H:i:s"));
            $questionMarks = "? AS `" . $this->m_hashDiffFieldName . "`,? AS `" . $this->m_hubHashFieldName . "`,? AS `" . $this->m_sourceFieldName . "`,? AS `" . $this->m_dateFieldName . "`";

            $data = array_change_key_case($a_satellite->getData());

            //go through each of the fields and add them to the query if the satellite has data for that field
            foreach ($this->m_fieldMap as $field => $column)
            {
                //if the satellite does not have that data
                if (!isset($data[$field]))
                {
                    continue;
                }

                $questionMarks .= ",? AS `" . $column . "`";
                $query .= ",`" . $column . "`";
                array_push($args, $data[$field]);
            }

            $query .= ") SELECT * FROM (SELECT " . $questionMarks . ") AS tmp ";
            $query .= "WHERE NOT EXISTS (SELECT 1 FROM `" . $this->m_tableName . "` WHERE `" . $this->m_hashDiffFieldName . "`=? AND `" . $this->m_hubHashFieldName . "`=? LIMIT 1) LIMIT 1";

            //prepend the query to the args
            array_unshift($args, $query);

            //add the where not exists values to the argument list
            array_push($args, $a_satellite->getHashDiff(), $a_satellite->getHubHash());

            $this->m_pdo->chooseConnection($this->m_connectionName);
            $result = call_user_func_array(array($this->m_pdo, "execute"), $args);

            //negative int means there was an error in the query
            if (is_int($result) && $result < 0)
            {
                return DV2_ERROR;
            }

            return DV2_SUCCESS;
        }
    }
?>