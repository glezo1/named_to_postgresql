import psycopg2
import subprocess
import ipaddress
import time

db_host         =   'localhost'
db_port         =   5432
db_user         =   'postgres'
db_pass         =   'postgres'
db_db           =   'dns'
bind_log_path   =   '/var/log/query.log'

#--------------------------------------------------------------------------------------------------------------------------------
def nslookup(query,server):
    cmd_output  =   None
    try:
        cmd_output  =   subprocess.check_output(['nslookup',query,server])
    except subprocess.CalledProcessError as e:
        cmd_output  =   e.output
    cmd_output  =   cmd_output.decode('utf-8')
    lines       =   cmd_output.split('\n')
    state       =   0
    for current_line in lines:
        if(state==0 and current_line.strip()==''):
            state   =   1
        elif(state==1 and 'Address:' in current_line):
            return current_line.split('Address:')[1].strip()
    return None
#--------------------------------------------------------------------------------------------------------------------------------
def process_pending_lines(conn,cur,list_of_tuples):
    result_new_pending  =   []
    for current_pending_entry in list_of_tuples:
        date_and_time       =   current_pending_entry[0]
        client_ip_str       =   current_pending_entry[1]
        client_query        =   current_pending_entry[2]
        server_ip_str       =   current_pending_entry[3]

        if(client_ip_str==server_ip_str):
            #this is a query made to itself to retrieve the authoritative answer that was given (or not) to the client. Ignore it, do not call nslookup or will enter a loop!
            pass
        else:
            server_answer_str   =   nslookup(client_query,server_ip_str)
            server_answer_int   =   None
            client_ip_int       =   int(ipaddress.IPv4Address(client_ip_str))
            server_ip_int       =   int(ipaddress.IPv4Address(server_ip_str))
            if(server_answer_str==None):
                server_answer_int   =   'NULL'
                server_answer_str   =   'NULL'
            else:
                server_answer_int   =   int(ipaddress.IPv4Address(server_answer_str))
                server_answer_str   =   "'"+server_answer_str+"'"
            sql_query       =   """
                                    INSERT INTO dns_log
                                    (
                                        log_time
                                        ,client_ip_string
                                        ,client_ip_int
                                        ,client_question
                                        ,server_ip_string
                                        ,server_ip_int
                                        ,server_answer_ip_string
                                        ,server_answer_ip_int
                                    )
                                    VALUES
                                    (
                                        TO_TIMESTAMP('"""+date_and_time+"""','DD-Mon-YYYY HH24:MI:SS.MS')
                                        ,'"""+client_ip_str+"""'
                                        ,"""+str(client_ip_int)+"""
                                        ,'"""+client_query+"""'
                                        ,'"""+server_ip_str+"""'
                                        ,"""+str(server_ip_int)+"""
                                        ,"""+server_answer_str+"""
                                        ,"""+str(server_answer_int)+"""
                                    )
                                    ON CONFLICT DO NOTHING
                                """
            #print(sql_query)
            try:
                cur.execute(sql_query)
                conn.commit()
            except Exception as e:
                print(e)
                result_new_pending.append(current_pending_entry)
    return result_new_pending
#--------------------------------------------------------------------------------------------------------------------------------
def main():
    conn            =    None
    cur             =    None
    tail_line       =   'tail -f '+bind_log_path
    process         =   subprocess.Popen(tail_line,stderr=subprocess.PIPE,stdout=subprocess.PIPE,shell=True)
    pending_lines   =   []
    while(True):
        if(conn==None):
            try:
                conn    =   psycopg2.connect("host='"+db_host+"' user='"+db_user+"' dbname='"+db_db+"' password='"+db_pass+"'")
                cur     =   conn.cursor()
            except Exception as e:
                print("I am unable to connect to the database")
                print(e)
                time.sleep(5)
        else:
            output_line =   process.stdout.readline()
            if(output_line):
                output_line =   output_line.decode('utf-8')
                print(output_line.strip())
                tokens          =   output_line.split(' ')
                date            =   tokens[0]
                time            =   tokens[1]
                client_ip_str   =   tokens[3].split('#')[0]
                client_query    =   tokens[5]
                server_ip_str   =   tokens[9].strip()[1:-1] #remove ( and )
                
                pending_lines.append((date+' '+time,client_ip_str,client_query,server_ip_str))
                pending_lines   =   process_pending_lines(conn,cur,pending_lines)
#--------------------------------------------------------------------------------------------------------------------------------
if(__name__=='__main__'):
    main()

