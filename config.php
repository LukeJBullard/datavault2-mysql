<?php
    /**
     * Datavault2 MySQL
     * Config File
     * 
     * @author Luke Bullard
     */
    
    //make sure we are included securely
    if (!defined("INPROCESS")) { header("HTTP/1.0 403 Forbidden"); exit(0); }
    
    /**
     * The configuration array for the DV2-MySQL Module
     * Parameters:
     *      millisecond_precision <boolean> If true, uses datetime(3) for load dates. Otherwise, uses datetime.
     */
    $dv2mysql_config = array(
        "millisecond_precision" => true
    );
?>