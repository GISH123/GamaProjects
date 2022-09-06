from info.common.tableinfo.TableInfoBasic import TableInfoBasic as TableInfoBasic


class TableInfo_BUReportStatistics(TableInfoBasic):

    @classmethod
    def getBUReportStatisticsInfo(self,makeInfo):
        tableauXMLStr = """ 
    <datasource caption='BU0000_遊戲數據指標' inline='true' name='federated.0ck0e7s0cd3dbj1bc0t3605tbcxx' version='18.1'>
      <connection class='federated'>
        <named-connections>
          <named-connection caption='[:ServerName]' name='greenplum.1w0ba4n047k4ez1e233671fmg3j7'>
            <connection class='greenplum' dbname='[:DBName]' odbc-native-protocol='' one-time-sql='' port='[:ServerPort]' server='[:ServerName]' sslmode='' username='[:UserName]' />
          </named-connection>
        </named-connections>
        <_.fcp.ObjectModelEncapsulateLegacy.false...relation connection='greenplum.1w0ba4n047k4ez1e233671fmg3j7' name='X__SQL___' type='text'>
          SELECT &#13;
            &quot;AA&quot;.&quot;reportname&quot; AS &quot;reportname&quot;,
            &quot;AA&quot;.&quot;startdate&quot; AS &quot;startdate&quot;,
            &quot;AA&quot;.&quot;enddate&quot; AS &quot;enddate&quot;,
            &quot;AA&quot;.&quot;periodtype&quot; AS &quot;periodtype&quot;,
            &quot;AA&quot;.&quot;gamename&quot; AS &quot;gamename&quot;,
            &quot;AA&quot;.&quot;reportcode&quot; AS &quot;reportcode&quot;,
            &quot;AA&quot;.&quot;datatype1&quot; AS &quot;datatype1&quot;,
            &quot;AA&quot;.&quot;datatype2&quot; AS &quot;datatype2&quot;,
            &quot;AA&quot;.&quot;datatype3&quot; AS &quot;datatype3&quot;,
            &quot;AA&quot;.&quot;datatype4&quot; AS &quot;datatype4&quot;,
            &quot;AA&quot;.&quot;datatype5&quot; AS &quot;datatype5&quot;,
            &quot;AA&quot;.&quot;value&quot; AS &quot;value&quot;
          FROM &quot;[:GameName]&quot;.&quot;[:DBName]statistics&quot; &quot;AA&quot;&#13;
          WHERE 1 = 1
        </_.fcp.ObjectModelEncapsulateLegacy.false...relation>
        <_.fcp.ObjectModelEncapsulateLegacy.true...relation connection='greenplum.1w0ba4n047k4ez1e233671fmg3j7' name='X__SQL___' type='text'>
          SELECT &#13;
            &quot;AA&quot;.&quot;reportname&quot; AS &quot;reportname&quot;,
            &quot;AA&quot;.&quot;startdate&quot; AS &quot;startdate&quot;,
            &quot;AA&quot;.&quot;enddate&quot; AS &quot;enddate&quot;,
            &quot;AA&quot;.&quot;periodtype&quot; AS &quot;periodtype&quot;,
            &quot;AA&quot;.&quot;gamename&quot; AS &quot;gamename&quot;,
            &quot;AA&quot;.&quot;reportcode&quot; AS &quot;reportcode&quot;,
            &quot;AA&quot;.&quot;datatype1&quot; AS &quot;datatype1&quot;,
            &quot;AA&quot;.&quot;datatype2&quot; AS &quot;datatype2&quot;,
            &quot;AA&quot;.&quot;datatype3&quot; AS &quot;datatype3&quot;,
            &quot;AA&quot;.&quot;datatype4&quot; AS &quot;datatype4&quot;,
            &quot;AA&quot;.&quot;datatype5&quot; AS &quot;datatype5&quot;,
            &quot;AA&quot;.&quot;value&quot; AS &quot;value&quot;
          FROM &quot;[:GameName]&quot;.&quot;[:DBName]statistics&quot; &quot;AA&quot;&#13;
          WHERE 1 = 1
        </_.fcp.ObjectModelEncapsulateLegacy.true...relation>
        <metadata-records>
          <metadata-record class='column'>
            <remote-name>reportname</remote-name>
            <remote-type>130</remote-type>
            <local-name>[reportname]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>reportname</remote-alias>
            <ordinal>1</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[[:DBName]statistics ([:GameName].[:DBName]statistics)_8632E3CEAEA247FD843794BB4DCB95AA]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>startdate</remote-name>
            <remote-type>7</remote-type>
            <local-name>[startdate]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>startdate</remote-alias>
            <ordinal>2</ordinal>
            <local-type>date</local-type>
            <aggregation>Year</aggregation>
            <contains-null>true</contains-null>
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_TYPE_DATE&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_TYPE_DATE&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[[:DBName]statistics ([:GameName].[:DBName]statistics)_8632E3CEAEA247FD843794BB4DCB95AA]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>enddate</remote-name>
            <remote-type>7</remote-type>
            <local-name>[enddate]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>enddate</remote-alias>
            <ordinal>3</ordinal>
            <local-type>date</local-type>
            <aggregation>Year</aggregation>
            <contains-null>true</contains-null>
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_TYPE_DATE&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_TYPE_DATE&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[[:DBName]statistics ([:GameName].[:DBName]statistics)_8632E3CEAEA247FD843794BB4DCB95AA]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>periodtype</remote-name>
            <remote-type>130</remote-type>
            <local-name>[periodtype]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>periodtype</remote-alias>
            <ordinal>4</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[[:DBName]statistics ([:GameName].[:DBName]statistics)_8632E3CEAEA247FD843794BB4DCB95AA]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>gamename</remote-name>
            <remote-type>130</remote-type>
            <local-name>[gamename]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>gamename</remote-alias>
            <ordinal>5</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[[:DBName]statistics ([:GameName].[:DBName]statistics)_8632E3CEAEA247FD843794BB4DCB95AA]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>reportcode</remote-name>
            <remote-type>130</remote-type>
            <local-name>[reportcode]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>reportcode</remote-alias>
            <ordinal>6</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[[:DBName]statistics ([:GameName].[:DBName]statistics)_8632E3CEAEA247FD843794BB4DCB95AA]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>datatype1</remote-name>
            <remote-type>130</remote-type>
            <local-name>[datatype1]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>datatype1</remote-alias>
            <ordinal>7</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[[:DBName]statistics ([:GameName].[:DBName]statistics)_8632E3CEAEA247FD843794BB4DCB95AA]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>datatype2</remote-name>
            <remote-type>130</remote-type>
            <local-name>[datatype2]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>datatype2</remote-alias>
            <ordinal>8</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[[:DBName]statistics ([:GameName].[:DBName]statistics)_8632E3CEAEA247FD843794BB4DCB95AA]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>datatype3</remote-name>
            <remote-type>130</remote-type>
            <local-name>[datatype3]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>datatype3</remote-alias>
            <ordinal>9</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[[:DBName]statistics ([:GameName].[:DBName]statistics)_8632E3CEAEA247FD843794BB4DCB95AA]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>datatype4</remote-name>
            <remote-type>130</remote-type>
            <local-name>[datatype4]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>datatype4</remote-alias>
            <ordinal>10</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[[:DBName]statistics ([:GameName].[:DBName]statistics)_8632E3CEAEA247FD843794BB4DCB95AA]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>datatype5</remote-name>
            <remote-type>130</remote-type>
            <local-name>[datatype5]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>datatype5</remote-alias>
            <ordinal>11</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[[:DBName]statistics ([:GameName].[:DBName]statistics)_8632E3CEAEA247FD843794BB4DCB95AA]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>value</remote-name>
            <remote-type>130</remote-type>
            <local-name>[value]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>value</remote-alias>
            <ordinal>12</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[[:DBName]statistics ([:GameName].[:DBName]statistics)_8632E3CEAEA247FD843794BB4DCB95AA]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
        </metadata-records>
      </connection>
      <aliases enabled='yes' />
      <_.fcp.ObjectModelTableType.true...column caption='[:DBName]statistics' datatype='table' name='[__tableau_internal_object_id__].[[:DBName]statistics ([:GameName].[:DBName]statistics)_8632E3CEAEA247FD843794BB4DCB95AA]' role='measure' type='quantitative' />
      <column caption='01_遊戲名稱' datatype='string' name='[gamename]' role='dimension' type='nominal' />
      <column caption='02_報表週期' datatype='string' name='[periodtype]' role='dimension' type='nominal' />
      <column caption='03_報表代碼' datatype='string' name='[reportcode]' role='dimension' type='nominal' />
      <column caption='04_報表周間' datatype='string' name='[reportname]' role='dimension' type='nominal' />
      <column caption='11_報表開始日期' datatype='date' name='[startdate]' role='dimension' type='ordinal' />
      <column caption='12_報表結束日期' datatype='date' name='[enddate]' role='dimension' type='ordinal' />
      <column caption='21_報表名稱' datatype='string' name='[datatype1]' role='dimension' type='nominal' />
      <column caption='22_國家' datatype='string' name='[datatype2]' role='dimension' type='nominal' />
      <column caption='31_資料細項一' datatype='string' name='[datatype3]' role='dimension' type='nominal' />
      <column caption='32_資料細項二' datatype='string' name='[datatype4]' role='dimension' type='nominal' />
      <column caption='33_資料細項三' datatype='string' name='[datatype5]' role='dimension' type='nominal' />
      <column caption='99_值' datatype='real' datatype-customized='true' name='[value]' role='measure' type='quantitative' />
      <layout _.fcp.SchemaViewerObjectModel.false...dim-percentage='0.5' _.fcp.SchemaViewerObjectModel.false...measure-percentage='0.4' _.fcp.SchemaViewerObjectModel.true...common-percentage='1' _.fcp.SchemaViewerObjectModel.true...user-set-layout-v2='true' dim-ordering='alphabetic' measure-ordering='alphabetic' show-structure='true' />
      <semantic-values>
        <semantic-value key='[Country].[Name]' value='&quot;Taiwan&quot;' />
      </semantic-values>
      <_.fcp.ObjectModelEncapsulateLegacy.true...object-graph>
        <objects>
          <object caption='[:DBName]statistics' id='[:DBName]statistics ([:GameName].[:DBName]statistics)_8632E3CEAEA247FD843794BB4DCB95AA'>
            <properties context=''>
              <relation connection='greenplum.1w0ba4n047k4ez1e233671fmg3j7' name='X__SQL___' type='text'>
                SELECT &#13;
                  &quot;AA&quot;.&quot;reportname&quot; AS &quot;reportname&quot;,
                  &quot;AA&quot;.&quot;startdate&quot; AS &quot;startdate&quot;,
                  &quot;AA&quot;.&quot;enddate&quot; AS &quot;enddate&quot;,
                  &quot;AA&quot;.&quot;periodtype&quot; AS &quot;periodtype&quot;,
                  &quot;AA&quot;.&quot;gamename&quot; AS &quot;gamename&quot;,
                  &quot;AA&quot;.&quot;reportcode&quot; AS &quot;reportcode&quot;,
                  &quot;AA&quot;.&quot;datatype1&quot; AS &quot;datatype1&quot;,
                  &quot;AA&quot;.&quot;datatype2&quot; AS &quot;datatype2&quot;,
                  &quot;AA&quot;.&quot;datatype3&quot; AS &quot;datatype3&quot;,
                  &quot;AA&quot;.&quot;datatype4&quot; AS &quot;datatype4&quot;,
                  &quot;AA&quot;.&quot;datatype5&quot; AS &quot;datatype5&quot;,
                  &quot;AA&quot;.&quot;value&quot; AS &quot;value&quot;
                FROM &quot;[:GameName]&quot;.&quot;[:DBName]statistics&quot; &quot;AA&quot;&#13;
                WHERE 1 = 1
              </relation>
            </properties>
          </object>
        </objects>
      </_.fcp.ObjectModelEncapsulateLegacy.true...object-graph>
    </datasource>
        """

        xmlReplaceArr = [
            ["[:ServerName]", makeInfo["serverName"]]
            , ["[:ServerPort]", makeInfo["serverPort"]]
            , ["[:DBName]", makeInfo["dbName"]]
            , ["[:UserName]", makeInfo["userName"]]
            , ["[:GameName]", makeInfo["schemaName"]]
        ]

        for xmlReplace in xmlReplaceArr:
            tableauXMLStr = tableauXMLStr.replace(xmlReplace[0], xmlReplace[1])

        return {"value":tableauXMLStr}









