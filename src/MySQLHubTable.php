<?php
    /**
     * Data Vault 2.0 Module for OliveWeb
     * Hub Table backed by MySQL for Storage
     * 
     * @author Luke Bullard
     */

    /**
     * A HubTable that saves Hubs to a MySQL Table
     */
    class MySQLHubTable extends HubTable
    {
        protected $m_tableName;
        protected $m_dataFieldName;
        protected $m_sourceFieldName;
        protected $m_loadDateFieldName;
        protected $m_hashKeyFieldName;
        protected $m_connectionName;
        protected $m_pdo;

        /**
         * Constructor for MySQLHubTable
         * 
         * @param String $a_connectionName The name of the Olive PDO connection to use when communicating with the MySQL database
         * @param String $a_tableName The name of the MySQL table to store the Hubs in
         * @param String $a_dataFieldName The name of the column in the MySQL table that stores the unique identifying data of the Hub
         * @param String $a_sourceFieldName The name of the column in the MySQL table that stores the initial source of the Hub
         * @param String $a_loadDateFieldName The name of the column in the MySQL table that stores the initial load date of the Hub
         * @param String $a_hashKeyFieldName The name of the column in the MySQL table that stores the hash of the Hub (Primary key)
         */
        public function __construct($a_connectionName, $a_tableName,$a_dataFieldName, $a_sourceFieldName, $a_loadDateFieldName, $a_hashKeyFieldName)
        {
            $this->m_tableName = $a_tableName;
            $this->m_dataFieldName = $a_dataFieldName;
            $this->m_sourceFieldName = $a_sourceFieldName;
            $this->m_loadDateFieldName = $a_loadDateFieldName;
            $this->m_hashKeyFieldName = $a_hashKeyFieldName;
            $this->m_connectionName = $a_connectionName;
            $this->m_pdo = Modules::getInstance()["pdo"];
        }

        /**
         * Saves the hub to the table. If the Hub already exists, skips it.
         * 
         * @param Hub $a_hub The Hub to save to the table
         * @return Int DV2_SUCCESS or DV2_ERROR
         */
        public function saveHub($a_hub)
        {
            //sprintf template string
            $query="INSERT
                    INTO `%s`
                    (`%s`, `%s`, `%s`, `%s`)
                    SELECT * FROM (SELECT ? AS `%s`, ? AS `%s`, ? AS `%s`, ? AS `%s`) AS tmp
                    
                    WHERE NOT EXISTS (
                        SELECT 1 FROM `%s` WHERE `%s` = ? LIMIT 1
                    )
                    LIMIT 1";
            
            //add column and table names in via sprintf
            $query = sprintf($query,
                $this->m_tableName,
                $this->m_dataFieldName, $this->m_sourceFieldName, $this->m_loadDateFieldName, $this->m_hashKeyFieldName,
                $this->m_dataFieldName, $this->m_sourceFieldName, $this->m_loadDateFieldName, $this->m_hashKeyFieldName,
                $this->m_tableName, $this->m_hashKeyFieldName
            );

            //execute query
            $this->m_pdo->chooseConnection($this->m_connectionName);
            $result = $this->m_pdo->execute($query,
                $a_hub->getData(), $a_hub->getSource(), date("Y-m-d H:i:s"), $a_hub->getHashKey(),
                $a_hub->getHashKey()
            );

            //error will return negative int
            if (is_int($result) && $result < 0)
            {
                return DV2_ERROR;
            }

            return DV2_SUCCESS;
        }

        /**
         * Returns if the hub already exists in the table
         * 
         * @param String $a_hashKey The Hash of the hub to look for
         * @return Boolean If the hub exists
         */
        public function hubExists($a_hashKey)
        {
            //sprintf template string
            $query = "SELECT 1 FROM `%s` WHERE `%s` = ? LIMIT 1";

            //add column and table names via sprintf
            $query = sprintf($query, $this->m_tableName, $this->m_hashKeyFieldName);

            //execute query
            $this->m_pdo->chooseConnection($this->m_connectionName);
            $result = $this->m_pdo->select($query, $a_hashKey);

            if (is_array($result) && !empty($result))
            {
                return true;
            }

            return false;
        }

        /**
         * Retrieves a hub from the table
         * 
         * @param String $a_hashKey The hash of the Hub to look for
         * @return Hub The retrieved Hub from the table
         * @return Int DV2_ERROR If the Hub could not be retrieved or does not exist
         */
        public function getHub($a_hashKey)
        {
            //sprintf template string
            $query = "SELECT `%s`, `%s`, `%s` FROM `%s` WHERE `%s` = ? LIMIT 1";

            //add column and table names via sprintf
            $query = sprintf($query,
                $this->m_dataFieldName, $this->m_sourceFieldName, $this->m_loadDateFieldName,
                $this->m_tableName,
                $this->m_hashKeyFieldName
            );

            //execute query
            $this->m_pdo->chooseConnection($this->m_connectionName);
            $result = $this->m_pdo->select($query, $a_hashKey);

            //error returns negative int
            if (is_int($result) && $result < 0)
            {
                return DV2_ERROR;
            }

            //if hub was found
            if (is_array($result) && !empty($result))
            {
                return new Hub(
                    $result[0][$this->m_sourceFieldName],
                    $result[0][$this->m_loadDateFieldName],
                    $result[0][$this->m_dataFieldName]
                );
            }

            //no hub found
            return DV2_ERROR;
        }

        /**
         * Deletes a Hub from the table
         * 
         * @param String $a_hashKey The Hash of the Hub to delete
         * @return Int DV2_SUCCESS or DV2_ERROR
         */
        public function clearHub($a_hashKey)
        {
            //sprintf template string
            $query = "DELETE FROM `%s` WHERE `%s` = ?";

            //add column and table names in via sprintf
            $query = sprintf($query, $this->m_tableName, $this->m_hashKeyFieldName);

            //execute query
            $result = $this->m_pdo->execute($query, $a_hashKey);
            
            //error returns negative int
            if (is_int($result) && $result < 0)
            {
                return DV2_ERROR;
            }

            return DV2_SUCCESS;
        }
    }
?>