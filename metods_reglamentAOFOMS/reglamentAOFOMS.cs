using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Xml;

public class reglamentAOFOMS
{
    public static bool handling_file(string file, List<clsConnections> link_connections, string[] folders, ReglamentLinker reglamentLinker, out string result_comments, out int count_row)
    {
        result_comments = string.Empty;
        bool result = false;
        count_row = 0;
        //ist<clsConnections> link_connections = new List<clsConnections>(); link_connections.Add(new clsConnections() { active = true, connectionString = "User ID=postgres;Password=123OOO321;Host=134.0.113.192;Port=5432", name = "postgres" });
        //-------------------------------------
        bool errors_free = true;
        string processing_comments = string.Empty;
        string filename = Path.GetFileName(file);
        string mnemonic = string.Empty;
        string idRequest = string.Empty;
        string typeVS = string.Empty;
        string version = null;
        object xml_object = null;
        List<string> list_errors = new List<string>();
        list_errors.Add(String.Format("Ошибка обработки файла - {0}", filename));
        try
        {
            mnemonic = reglamentLinker.getMnemonic();
            typeVS = reglamentLinker.link.comment;
            XmlDocument HEADER = new XmlDocument();
            if (!clsLibrary.getTeg_HEADER(file, ref HEADER, ref version))
            {
                errors_free = false; //result = true;
                throw new System.ArgumentException(string.Format("Не установлена версия файла данного вида сведений: '{0}'", typeVS));
            }

            reglamentLinker.getLink(null, null, version, Path.GetFileName(file));
            if (reglamentLinker.link == null)
            {
                errors_free = false; //result = true;
                throw new System.ArgumentException(string.Format("Не зарегистрирована версия '{0}' для вида сведений: '{1}'", version, typeVS));
            }

            if (clsLibrary.execQuery_PGR_getString(ref link_connections, "postgres", String.Format("select id from buf_eir.request where filename = '{0}' and fail = false and state is not null", filename.ToUpper())) != null)
            {
                errors_free = false; //result = true;
                throw new System.ArgumentException("Файл принят ранее, имена файлов должны быть уникальны");
            }

            xml_object = Schemes_AOFOMS.XML_abstract_file.FromXml(
                new FileStream(file, FileMode.Open, FileAccess.Read),
                reglamentLinker.link.schemaClassRoot, 
                Path.Combine(Path.Combine(Directory.GetParent(folders[1]).Parent.FullName, "schemes", reglamentLinker.link.nameSchema + ".xsd")), 
                mnemonic
            );
            GC.Collect();
            errors_free = (xml_object as Schemes_AOFOMS.XML_abstract_file)._Errors.Count == 0;
            result = true;
            if (errors_free)
            {
                idRequest = clsLibrary.execQuery_PGR_getString(ref link_connections, "postgres"
                    , String.Format("SELECT buf_eir.insert_request('{0}','{1}','{2}','{3}')", HEADER.InnerXml, mnemonic, reglamentLinker.link.nameSchema, Path.GetFileName(file).ToUpper()));
                if (idRequest == "-1" || idRequest == "0") { idRequest = string.Empty; throw new System.ArgumentException("Ошибка регистрации запроса"); }
                #region Запуск метода импорта в буфер                         
                switch (reglamentLinker.link.nameSchema)
                {
                    case "si_schema_1_0":
                        result = processingRequest_si_schema_1_0(ref link_connections, "postgres", idRequest, ref xml_object, mnemonic, out count_row, out processing_comments, 1000);
                        break;
                    case "zldn_schema_2_0":
                        result = processingRequest_zldn_schema_2_0(ref link_connections, "postgres", idRequest, ref xml_object, mnemonic, out count_row, out processing_comments, 1000);
                        break;
                    case "zldn_schema_2_1":
                        result = processingRequest_zldn_schema_2_1(ref link_connections, "postgres", idRequest, ref xml_object, mnemonic, out count_row, out processing_comments, 1000);
                        break;
                    default:
                        result = false;
                        throw new System.ArgumentException("Неизвестен метод обработки бизнес данных");
                }
                #endregion
            }
        }
        catch (Exception e)
        {
            if (!errors_free) list_errors.Add(e.Message);
            else { result = false; processing_comments = e.Message; }
        }
        if (/*result &&*/ !errors_free)
        {
            try
            {
                if (xml_object != null && (xml_object as Schemes_AOFOMS.XML_abstract_file)._Errors.Count > 0)
                    foreach (object pr in (xml_object as Schemes_AOFOMS.XML_abstract_file)._Errors)
                    {
                        list_errors.Add((pr as Schemes_AOFOMS.Protocol).N_REC + ", " +//(pr as Sp.XML.PR).OSHIB,
                            (pr as Schemes_AOFOMS.Protocol).IM_POL + " - " + (pr as Schemes_AOFOMS.Protocol).COMMENT);
                    }
                clsLibrary.createFileTXT_FromList(list_errors, Path.Combine(folders[1], string.Format(@"fail{0}-{1}.txt", mnemonic, Path.GetFileNameWithoutExtension(filename).ToUpper())));
            }
            catch
            {
                result = false;
                processing_comments = " ошибка! : Формирование файла с ответом по ошибкам";
            }
        }
        //меняем статус обработки пакета, если он был зарегистрирован
        if (idRequest != string.Empty)
            clsLibrary.execQuery_PGR(ref link_connections, "postgres",
                (result) ?
                    string.Format("update buf_eir.request set state = {0}, count_row = {1} where id = '{2}'", (errors_free) ? "0" : "-1", count_row, idRequest)
                    : string.Format("update buf_eir.request set fail = true where id = '{0}'", idRequest)
                );
        if (!result) result_comments += result_comments + processing_comments + count_row.ToString();
        return result;
    }
    /*public static int handling_file_old(string file, List<clsConnections> link_connections, string reglament_connections, string[] folders, ReglamentLinker reglamentLinker, out string result_comments)
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
            if (header == null) throw new System.ArgumentException("UNKNOWN VERSION");
            version = (header as XML_Universal.HEADER_files).VERSION;
            if (version == null) throw new System.ArgumentException("UNKNOWN VERSION");
            //type = linker.getType(null, Path.GetFileName(file), version);
            if (reglamentLinker.link.schemaClassRoot == null) throw new System.ArgumentException("UNKNOWN VERSION"); //Неизвестная версия
            #endregion
            //Чтение файла
            using (FileStream filestream = new FileStream(file, FileMode.Open, FileAccess.Read))
                xml_object =  Schemes_AOFOMS.XML_abstract_file.FromXml(filestream, reglamentLinker.link.schemaClassRoot, reglamentLinker.link.nameSchema);
            if (!(xml_object as Schemes_AOFOMS.XML_abstract_file).flk_check()) throw new System.ArgumentException("ERROR FLK");

            #region Регистрация файла в REQUEST (HEADER)

            //      0 - не может быть обработан, так как ранее представлялся
            //     -1 - ошибка в методе сервиса, откат!
            //   GUID - idRequest зарегистрированного запроса

            idRequest = clsLibrary.execQuery_getString(ref link_connections, reglament_connections, "eir"
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
                    count_row = importToBuffer_zldn_schema_1_0(ref link_connections, reglament_connections, "eir"
                        , idRequest
                        , "insert into buf_dn (id_request,fam,im,ot,w,dr,n_rec,enp,mcode,vpolis,spolis,npolis,snils,DOCTYPE,DOCSER,DOCNUM,PHONE,EMAIL,COMENT, DNBEG, DNEND, DNFACT) values "
                        , ref xml_object
                        , mnemonic
                        , 100);
                    if (count_row == -1) throw new System.ArgumentException("ERROR METOD");
                    break;
                case "zldn_schema_2_0":
                    count_row = importToBuffer_zldn_schema_2_0(ref link_connections, reglament_connections, "eir"
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
            clsLibrary.execQuery(ref link_connections, reglament_connections, "eir"
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
*/
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
            if (!clsLibrary.execQuery(ref link_connections, reglament_connections, "eir"
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

    /*public static int importToBuffer_zldn_schema_2_0(ref List<clsConnections> link_connections, string reglament_connections, string database, string idRequest, string query, ref object _object, string mnemonic, int limit_transaction = 1)
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
    */
    public static bool processingRequest_si_schema_1_0(ref List<clsConnections> link_connections, string database, string idRequest, ref object _object, string mnemonic, out int count_row, out string comments, int limit_transaction = 1)
    //выполнение запроса на вставку данных из объекта
    //возвращает -1 при ошибке
    {
        bool result = false;
        comments = string.Empty;
        List<string> values = new List<string>();
        count_row = (_object as Schemes_AOFOMS.si_schema_1_0.PROFIL).ZL.Length;
        if (_object == null || count_row == 0)
        {
            comments = "Отсутствуют бизнес данные";
            return result;
        }
        try
        {
            for (int row = 0; row < count_row; ++row)
            {
                values.Add(
                    " '" + idRequest +
                    "','" + mnemonic +
                    "','" + (_object as Schemes_AOFOMS.si_schema_1_0.PROFIL).HEADER.YEAR +
                    "','" + (_object as Schemes_AOFOMS.si_schema_1_0.PROFIL).ZL[row].FAM +
                    "','" + (_object as Schemes_AOFOMS.si_schema_1_0.PROFIL).ZL[row].IM +
                    "','" + (_object as Schemes_AOFOMS.si_schema_1_0.PROFIL).ZL[row].OT +
                    "','" + (_object as Schemes_AOFOMS.si_schema_1_0.PROFIL).ZL[row].W +
                    "','" + (_object as Schemes_AOFOMS.si_schema_1_0.PROFIL).ZL[row].DR.ToString("yyyyMMdd") +
                    "','" + (_object as Schemes_AOFOMS.si_schema_1_0.PROFIL).ZL[row].N_REC +
                    "','" + (_object as Schemes_AOFOMS.si_schema_1_0.PROFIL).ZL[row].ENP +
                    "','" + (_object as Schemes_AOFOMS.si_schema_1_0.PROFIL).ZL[row].SMOCODE +
                    "','" + (_object as Schemes_AOFOMS.si_schema_1_0.PROFIL).ZL[row].VPOLIS +
                    "','" + (_object as Schemes_AOFOMS.si_schema_1_0.PROFIL).ZL[row].SPOLIS +
                    "','" + (_object as Schemes_AOFOMS.si_schema_1_0.PROFIL).ZL[row].NPOLIS +
                    "','" + (_object as Schemes_AOFOMS.si_schema_1_0.PROFIL).ZL[row].SNILS +
                    "','" + (_object as Schemes_AOFOMS.si_schema_1_0.PROFIL).ZL[row].DOCTYPE +
                    "','" + (_object as Schemes_AOFOMS.si_schema_1_0.PROFIL).ZL[row].DOCSER +
                    "','" + (_object as Schemes_AOFOMS.si_schema_1_0.PROFIL).ZL[row].DOCNUM +
                    "','" + (_object as Schemes_AOFOMS.si_schema_1_0.PROFIL).ZL[row].FORM +
                    "','" + (_object as Schemes_AOFOMS.si_schema_1_0.PROFIL).ZL[row].KVART +
                    "','" + (_object as Schemes_AOFOMS.si_schema_1_0.PROFIL).ZL[row].COMENTZ +
                    "'"
                    );
            }
            result = clsLibrary.execQuery_PGR_insertList(ref link_connections, database,
                "insert into buf_eir.buf_si (id_request,mcode,year,fam,im,ot,w,dr,n_rec,enp,smocode,vpolis,spolis,npolis,snils,DOCTYPE,DOCSER,DOCNUM,FORM,KVART,COMENTZ) values ",
                values, 500);
        }
        catch (Exception e)
        {
            comments = "Ошибка вставки бизнес данных";
        }
        return result;
    }
    public static bool processingRequest_zldn_schema_2_0(ref List<clsConnections> link_connections, string database, string idRequest, ref object _object, string mnemonic, out int count_row, out string comments, int limit_transaction = 1)
    {
        bool result = false;
        comments = string.Empty;
        List<string> values = new List<string>();
        count_row = (_object as Schemes_AOFOMS.zldn_schema_2_0.ZLDN).ZL.Length;
        if (_object == null || count_row == 0)
        {
            comments = "Отсутствуют бизнес данные";
            return result;
        }
        try
        {
            string DNBEG, DNPLAN, DNEND, DNFACT;

            for (int row = 0; row < count_row; ++row)
            {
                DNBEG = XmlHelper.SerializeClear<Schemes_AOFOMS.zldn_schema_2_0.DNBEG[]>((_object as Schemes_AOFOMS.zldn_schema_2_0.ZLDN).ZL[row].DNBEG);
                DNPLAN = XmlHelper.SerializeClear<Schemes_AOFOMS.zldn_schema_2_0.DNPLAN[]>((_object as Schemes_AOFOMS.zldn_schema_2_0.ZLDN).ZL[row].DNPLAN);
                DNEND = XmlHelper.SerializeClear<Schemes_AOFOMS.zldn_schema_2_0.DNEND[]>((_object as Schemes_AOFOMS.zldn_schema_2_0.ZLDN).ZL[row].DNEND);
                DNFACT = XmlHelper.SerializeClear<Schemes_AOFOMS.zldn_schema_2_0.DNFACT[]>((_object as Schemes_AOFOMS.zldn_schema_2_0.ZLDN).ZL[row].DNFACT);
                values.Add(
                    " '" + idRequest +
                    "','" + mnemonic +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_2_0.ZLDN).ZL[row].FAM +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_2_0.ZLDN).ZL[row].IM +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_2_0.ZLDN).ZL[row].OT +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_2_0.ZLDN).ZL[row].W +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_2_0.ZLDN).ZL[row].DR.ToString("yyyyMMdd") +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_2_0.ZLDN).ZL[row].N_REC +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_2_0.ZLDN).ZL[row].ENP +
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
                    "," + ((DNFACT == null) ? "null" : "'" + DNFACT + "'"));

            }
            result = clsLibrary.execQuery_PGR_insertList(ref link_connections, database,
                "insert into buf_eir.buf_dn(id_request, mcode, fam, im, ot, w, dr, n_rec, enp, vpolis, spolis, npolis, snils, DOCTYPE, DOCSER, DOCNUM, PHONE, EMAIL, COMENT, DNBEG, DNPLAN, DNEND, DNFACT) values ",
                values, 500);
        }
        catch { }
        return result;
    }
    public static bool processingRequest_zldn_schema_2_1(ref List<clsConnections> link_connections, string database, string idRequest, ref object _object, string mnemonic, out int count_row, out string comments, int limit_transaction = 1)
    {
        bool result = false;
        comments = string.Empty;
        List<string> values = new List<string>();
        count_row = (_object as Schemes_AOFOMS.zldn_schema_2_1.ZLDN).ZL.Length;
        if (_object == null || count_row == 0)
        {
            comments = "Отсутствуют бизнес данные";
            return result;
        }
        try
        {
            string DNBEG, DNPLAN, DNEND, DNFACT;

            for (int row = 0; row < count_row; ++row)
            {
                DNBEG = XmlHelper.SerializeClear<Schemes_AOFOMS.zldn_schema_2_1.DNBEG>((_object as Schemes_AOFOMS.zldn_schema_2_1.ZLDN).ZL[row].DNBEG);
                DNPLAN = XmlHelper.SerializeClear<Schemes_AOFOMS.zldn_schema_2_1.DNPLAN>((_object as Schemes_AOFOMS.zldn_schema_2_1.ZLDN).ZL[row].DNPLAN);
                DNEND = XmlHelper.SerializeClear<Schemes_AOFOMS.zldn_schema_2_1.DNEND>((_object as Schemes_AOFOMS.zldn_schema_2_1.ZLDN).ZL[row].DNEND);
                DNFACT = XmlHelper.SerializeClear<Schemes_AOFOMS.zldn_schema_2_1.DNFACT>((_object as Schemes_AOFOMS.zldn_schema_2_1.ZLDN).ZL[row].DNFACT);
                values.Add(
                    " '" + idRequest +
                    "','" + mnemonic +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_2_1.ZLDN).ZL[row].FAM +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_2_1.ZLDN).ZL[row].IM +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_2_1.ZLDN).ZL[row].OT +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_2_1.ZLDN).ZL[row].W +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_2_1.ZLDN).ZL[row].DR.ToString("yyyyMMdd") +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_2_1.ZLDN).ZL[row].N_REC +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_2_1.ZLDN).ZL[row].ENP +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_2_1.ZLDN).ZL[row].VPOLIS +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_2_1.ZLDN).ZL[row].SPOLIS +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_2_1.ZLDN).ZL[row].NPOLIS +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_2_1.ZLDN).ZL[row].SNILS +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_2_1.ZLDN).ZL[row].DOCTYPE +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_2_1.ZLDN).ZL[row].DOCSER +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_2_1.ZLDN).ZL[row].DOCNUM +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_2_1.ZLDN).ZL[row].PHONE +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_2_1.ZLDN).ZL[row].EMAIL +
                    "','" + (_object as Schemes_AOFOMS.zldn_schema_2_1.ZLDN).ZL[row].COMENTZ +
                    "'," + ((DNBEG == null) ? "null" : "'" + DNBEG + "'") +
                    "," + ((DNPLAN == null) ? "null" : "'" + DNPLAN + "'") +
                    "," + ((DNEND == null) ? "null" : "'" + DNEND + "'") +
                    "," + ((DNFACT == null) ? "null" : "'" + DNFACT + "'"));

            }
            result = clsLibrary.execQuery_PGR_insertList(ref link_connections, database,
                "insert into buf_eir.buf_dn(id_request, mcode, fam, im, ot, w, dr, n_rec, enp, vpolis, spolis, npolis, snils, DOCTYPE, DOCSER, DOCNUM, PHONE, EMAIL, COMENT, DNBEG, DNPLAN, DNEND, DNFACT) values ",
                values, 500);
        }
        catch { }
        return result;
    }

}
