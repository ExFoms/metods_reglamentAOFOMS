using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;

public class reglamentAOFOMS
{
    public static int handling_file_aofoms(string file, List<clsConnections> link_connections, string reglament_connections, string[] folders, ReglamentLinker reglamentLinker, out string result_comments)
    // Обработка файлов Регламентов АОФОМС 
    {
        result_comments = "Ok";
        int result = -1;
        object xml_object = null;
        string filename = Path.GetFileName(file);
        //string filemask1 = linker.getMask_ByFileName(filename);
        string mnemonic = null;
        string version = null;
        //Type type = null;
        string idRequest = "-1";
        string prefix = "fail";
        try
        {
            #region Определение файла и версии
            if (!File.Exists(file)) throw new System.ArgumentException("FILE NOT FIND"); //Файл не найден 
            if (reglamentLinker.link.filemask == null) throw new System.ArgumentException("UNKNOWN FILE"); //дублирование проверки наружного процесса обработки файлов
            int index1 = reglamentLinker.link.filemask.IndexOf("#");
            string separator = reglamentLinker.link.filemask.Substring(index1 + 1, 1);
            int index_separator = filename.IndexOf(separator);
            mnemonic = filename.Substring(index1 + 1, index_separator - index1 - 1);
            string headerTxt = null;
            object header = clsLibrary.find_HEADERtegInFile(file, ref headerTxt);
            if (header == null) throw new System.ArgumentException("UNKNOWN VERSION"/*"Не найден тег <HEADER>!"*/);
            version = (header as XML_Universal.HEADER_files).VERSION;
            if (version == null) throw new System.ArgumentException("UNKNOWN VERSION"/*"Не найден тег <VERSION>!"*/);
            //type = linker.getType(null, Path.GetFileName(file), version);
            if (reglamentLinker.link.schemaClassRoot == null) throw new System.ArgumentException("UNKNOWN VERSION"); //Неизвестная версия
            #endregion

            //Чтение файла
            using (FileStream filestream = new FileStream(file, FileMode.Open, FileAccess.Read))
                xml_object = Schemes_AOFOMS.XML_abstract_file.FromXml(filestream, reglamentLinker.link.schemaClassRoot, reglamentLinker.link.nameSchema);

            if (!(xml_object as Schemes_AOFOMS.XML_abstract_file).flk_check()) throw new System.ArgumentException("ERROR FLK");

            #region Регистрация файла в REQUEST (HEADER)

            //      0 - не может быть обработан, так как ранее представлялся
            //     -1 - ошибка в методе сервиса, откат!
            //   GUID - idRequest зарегистрированного запроса

            idRequest = clsLibrary.execQuery_getString(
                ref link_connections
                , reglament_connections
                , "eir"
                , String.Format("EXEC eir.dbo.insert_newRequest '{0}','{1}','{2}','{3}'", Path.GetFileName(file).ToUpper(), mnemonic, reglamentLinker.link.nameSchema, headerTxt)
                );
            if (idRequest == "0") throw new System.ArgumentException("EXIST");
            if (idRequest == "-1") throw new System.ArgumentException("ERROR METOD");
            #endregion

            #region Запуск метода импорта в буфер
            int count_row = -1;
            switch (reglamentLinker.link.nameSchema + '_' + version.Replace('.', '_'))
            {
                case "zldn_schema_1_0":
                    count_row = importToBuffer_zldn_schema_1_0(
                        ref link_connections, reglament_connections, "eir"
                        , idRequest
                        , "insert into buf_dn (id_request,fam,im,ot,w,dr,n_rec,enp,mcode,vpolis,spolis,npolis,snils,DOCTYPE,DOCSER,DOCNUM,PHONE,EMAIL,COMENT, DNBEG, DNEND, DNFACT) values "
                        , ref xml_object
                        , mnemonic
                        , 100);
                    if (count_row == -1) throw new System.ArgumentException("ERROR METOD");
                    break;
                case "zldn_schema_2_0":
                    count_row = importToBuffer_zldn_schema_2_0(
                        ref link_connections, reglament_connections, "eir"
                        , idRequest
                        , "insert into buf_dn (id_request,fam,im,ot,w,dr,n_rec,enp,mcode,vpolis,spolis,npolis,snils,DOCTYPE,DOCSER,DOCNUM,PHONE,EMAIL,COMENT, DNBEG, DNPLAN, DNEND, DNFACT) values "
                        , ref xml_object
                        , mnemonic
                        , 100);
                    if (count_row == -1) throw new System.ArgumentException("ERROR METOD");
                    break;
                default:
                    throw new System.ArgumentException("ERROR METOD"); //Отсутствует метод обработки пакета
            }
            #endregion
            prefix = "ok"; result = count_row;
            clsLibrary.execQuery(
                ref link_connections, reglament_connections, "eir"
                , String.Format("update request set state = 0, count_row = {0} where id = '{1}'", count_row, idRequest));
        }
        catch (Exception e)
        {
            #region Отработка ошибок при чтении и регистрации файла
            result_comments = e.Message;
            List<string> list = new List<string>();
            list.Add(filename);
            prefix = "fail";
            switch (e.Message)
            {
                case "ERROR FLK":
                    list.Add(String.Format("Маска файла '{1}', версия '{2}', схема '{0}'.  \r\nНе соответствует требованиям схемы.", reglamentLinker.link.nameSchema, reglamentLinker.link.filemask, version));
                    foreach (Schemes_AOFOMS.Protocol protocol in (xml_object as Schemes_AOFOMS.XML_abstract_file)._Errors) list.Add(protocol.COMMENT);
                    break;
                case "UNKNOWN VERSION":
                    list.Add("Неизвестная версия!");
                    break;
                case "UNKNOWN FILE":
                    prefix = "UNKNOWN FILE"; //не трогаем файл
                    break;
                case "FILE NOT FIND":
                    list.Add("Файл не найден!");
                    break;
                case "EXIST":
                    list.Add("Файл был представлен ранее!");
                    break;
                //-----------------------------------------------------
                case "ERROR METOD":
                    prefix = "iterate";
                    break;
                default:
                    prefix = "iterate";
                    list.Add(e.Message);
                    break;
                    //перемещение файла в папку ошибок
            }
            //if (prefix != "iterate") 
            clsLibrary.createFileTXT_FromList(
                list,
                Path.Combine(folders[1], string.Format(@"{0}{1}-{2}.txt", prefix, mnemonic, Path.GetFileNameWithoutExtension(file).ToUpper())));
            if (idRequest != "-1" && idRequest != "0")
                clsLibrary.execQuery(
                    ref link_connections
                    , reglament_connections
                    , "eir"
                    , String.Format("update request set fail = 1 where id = '{0}'", idRequest));
            #endregion
        }
        if (prefix != "UNKNOWN FILE") clsLibrary.moveFile_byPrefix(file, prefix); //оставляем файл
        GC.Collect();
        return result;
    }

    public static int importToBuffer_zldn_schema_1_0(ref List<clsConnections> link_connections, string reglament_connections, string database, string idRequest, string query, ref object _object, string mnemonic, int limit_transaction = 1)
    //выполнение запроса на вставку данных из объекта
    //возвращает -1 при ошибке
    {
        int result = -1; //Ошибка метода
        int count = 0;
        string values = string.Empty;
        int count_row = (_object as Schemes_AOFOMS.zldn_schema_1_0.ZLDN).ZL.Length;
        if (_object == null || count_row == 0) return result;
        try
        {
            string DNBEG, DNEND, DNFACT;
            string connection_name = "";
            clsLibrary.get_stringSplitPos(ref connection_name, reglament_connections, ';', 0);
            SqlConnection connection = new SqlConnection(link_connections.Find(x => x.name == connection_name).connectionString + ";database=" + database);
            connection.Open();
            SqlCommand sqlCommand = connection.CreateCommand();
            SqlTransaction sqlTransaction = connection.BeginTransaction("SampleTransaction");
            sqlCommand.Connection = connection;
            //sqlCommand.CommandTimeout = 600000;
            sqlCommand.Transaction = sqlTransaction;
            for (int row = 0; row < count_row; ++row)
            {
                ++count;
                if (count != 1) values += ",";
                DNBEG = XmlHelper.SerializeClear<Schemes_AOFOMS.zldn_schema_1_0.DNBEG>((_object as Schemes_AOFOMS.zldn_schema_1_0.ZLDN).ZL[row].DNBEG);
                DNEND = XmlHelper.SerializeClear<Schemes_AOFOMS.zldn_schema_1_0.DNEND>((_object as Schemes_AOFOMS.zldn_schema_1_0.ZLDN).ZL[row].DNEND);
                DNFACT = XmlHelper.SerializeClear<Schemes_AOFOMS.zldn_schema_1_0.DNFACT>((_object as Schemes_AOFOMS.zldn_schema_1_0.ZLDN).ZL[row].DNFACT);
                values +=
                    " ('" + idRequest +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_1_0.ZLDN).ZL[row].FAM +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_1_0.ZLDN).ZL[row].IM +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_1_0.ZLDN).ZL[row].OT +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_1_0.ZLDN).ZL[row].W +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_1_0.ZLDN).ZL[row].DR.ToString("yyyyMMdd") +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_1_0.ZLDN).ZL[row].N_REC +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_1_0.ZLDN).ZL[row].ENP +
                    "','" + mnemonic +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_1_0.ZLDN).ZL[row].VPOLIS +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_1_0.ZLDN).ZL[row].SPOLIS +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_1_0.ZLDN).ZL[row].NPOLIS +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_1_0.ZLDN).ZL[row].SNILS +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_1_0.ZLDN).ZL[row].DOCTYPE +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_1_0.ZLDN).ZL[row].DOCSER +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_1_0.ZLDN).ZL[row].DOCNUM +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_1_0.ZLDN).ZL[row].PHONE +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_1_0.ZLDN).ZL[row].EMAIL +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_1_0.ZLDN).ZL[row].COMENTZ +
                    "'," + ((DNBEG == null) ? "null" : "'" + DNBEG + "'") +
                    "," + ((DNEND == null) ? "null" : "'" + DNEND + "'") +
                    "," + ((DNFACT == null) ? "null" : "'" + DNFACT + "'") +
                    ")";
                if (count == limit_transaction || row == count_row - 1)
                {
                    sqlCommand.CommandText = query + values;
                    sqlCommand.ExecuteNonQuery();
                    values = string.Empty;
                    count = 0;
                }
            }
            // дополняем тег DNPLAN из тега DNBEG, т.к. старая версия включала данные DNPLAN в DNBEG
            sqlTransaction.Commit();
            connection.Close();
            if (!clsLibrary.execQuery(
                    ref link_connections
                    , reglament_connections
                    , "eir"
                    , String.Format(
                        "update [eir].[dbo].[buf_dn] set DNPLAN = case when dnbeg is not null then (SELECT "
                        + "dnbeg.value('(/DNBEG/DS)[1]','varchar(100)') DS "
                        + ",dnbeg.value('(/DNBEG/MRSPEC)[1]','varchar(100)') MRSPEC,dnbeg.value('(/DNBEG/MRFIO)[1]','varchar(100)') MRFIO "
                        + ",dnbeg.value('(/DNBEG/MRSNILS)[1]','varchar(100)') MRSNILS,dnbeg.value('(/DNBEG/DOPLANY)[1]','varchar(100)') DOPLANY "
                        + ",dnbeg.value('(/DNBEG/DOPLANM)[1]','varchar(100)') DOPLANM,dnbeg.value('(/DNBEG/DOMESTO)[1]','varchar(100)') DOMESTO "
                        + "for XML PATH ('DNPLAN') ) else null end where id_request = '{0}'", idRequest)
            )) throw new System.ArgumentException("ERROR UPDATE TABLE");

            result = count_row;
        }
        catch
        {

        }
        return result;
    }

    public static int importToBuffer_zldn_schema_2_0(ref List<clsConnections> link_connections, string reglament_connections, string database, string idRequest, string query, ref object _object, string mnemonic, int limit_transaction = 1)
    //выполнение запроса на вставку данных из объекта
    //возвращает -1 при ошибке
    {
        int result = -1; //Ошибка метода
        int count = 0;
        string values = string.Empty;
        int count_row = (_object as Schemes_AOFOMS.zldn_schema_2_0.ZLDN).ZL.Length;
        if (_object == null || count_row == 0) return result;
        try
        {
            string DNBEG, DNPLAN, DNEND, DNFACT;
            string connection_name = "";
            clsLibrary.get_stringSplitPos(ref connection_name, reglament_connections, ';', 0);
            SqlConnection connection = new SqlConnection(link_connections.Find(x => x.name == connection_name).connectionString + ";database=" + database);
            connection.Open();
            SqlCommand sqlCommand = connection.CreateCommand();
            SqlTransaction sqlTransaction = connection.BeginTransaction("SampleTransaction");
            sqlCommand.Connection = connection;
            //sqlCommand.CommandTimeout = 600000;
            sqlCommand.Transaction = sqlTransaction;
            for (int row = 0; row < count_row; ++row)
            {
                ++count;
                if (count != 1) values += ",";
                DNBEG = XmlHelper.SerializeClear<Schemes_AOFOMS.zldn_schema_2_0.DNBEG>((_object as Schemes_AOFOMS.zldn_schema_2_0.ZLDN).ZL[row].DNBEG);
                DNPLAN = XmlHelper.SerializeClear<Schemes_AOFOMS.zldn_schema_2_0.DNPLAN>((_object as Schemes_AOFOMS.zldn_schema_2_0.ZLDN).ZL[row].DNPLAN);
                DNEND = XmlHelper.SerializeClear<Schemes_AOFOMS.zldn_schema_2_0.DNEND>((_object as Schemes_AOFOMS.zldn_schema_2_0.ZLDN).ZL[row].DNEND);
                DNFACT = XmlHelper.SerializeClear<Schemes_AOFOMS.zldn_schema_2_0.DNFACT>((_object as Schemes_AOFOMS.zldn_schema_2_0.ZLDN).ZL[row].DNFACT);
                values +=
                    " ('" + idRequest +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_2_0.ZLDN).ZL[row].FAM +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_2_0.ZLDN).ZL[row].IM +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_2_0.ZLDN).ZL[row].OT +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_2_0.ZLDN).ZL[row].W +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_2_0.ZLDN).ZL[row].DR.ToString("yyyyMMdd") +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_2_0.ZLDN).ZL[row].N_REC +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_2_0.ZLDN).ZL[row].ENP +
                    "','" + mnemonic +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_2_0.ZLDN).ZL[row].VPOLIS +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_2_0.ZLDN).ZL[row].SPOLIS +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_2_0.ZLDN).ZL[row].NPOLIS +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_2_0.ZLDN).ZL[row].SNILS +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_2_0.ZLDN).ZL[row].DOCTYPE +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_2_0.ZLDN).ZL[row].DOCSER +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_2_0.ZLDN).ZL[row].DOCNUM +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_2_0.ZLDN).ZL[row].PHONE +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_2_0.ZLDN).ZL[row].EMAIL +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_2_0.ZLDN).ZL[row].COMENTZ +
                    "'," + ((DNBEG == null) ? "null" : "'" + DNBEG + "'") +
                    "," + ((DNPLAN == null) ? "null" : "'" + DNPLAN + "'") +
                    "," + ((DNEND == null) ? "null" : "'" + DNEND + "'") +
                    "," + ((DNFACT == null) ? "null" : "'" + DNFACT + "'") +
                    ")";
                if (count == limit_transaction || row == count_row - 1)
                {
                    sqlCommand.CommandText = query + values;
                    sqlCommand.ExecuteNonQuery();
                    values = string.Empty;
                    count = 0;
                }
            }
            sqlTransaction.Commit();
            connection.Close();
            result = count_row;
        }
        catch { }
        return result;
    }
}
