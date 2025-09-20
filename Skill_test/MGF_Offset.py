import sys
import os
from pathlib import Path
from mslib import console_logger, executors, logger, level, date_time_tag, db_connector, DatabaseMs, PathFinder, UpdateMgfOffsetUnit

version = "v1.01"
work_dir = ""
db_connection_manager = db_connector.get_database_m_dbwriter()
database_ms = DatabaseMs(db_connection_manager)

def main():
    print("=================================================================================================")
    print("MGF Offset (" + version + ")")

    if len(sys.argv) != 1:
        print("command: python MGF_Offset.py workDir")
        return

    console_logger.set_console_logger(level.INFO)

    logger.get_root_logger().info("MGF Offset (" + version + ")")
    logger.get_root_logger().debug("workDir:" + sys.argv[1])

    work_dir = sys.argv[1]

    if not os.path.exists(work_dir):
        logger.get_root_logger().info("workDir:" + work_dir + " doesn't exsit. Creating the folder")
        os.makedirs(work_dir)
    work_dir = os.path.abspath(work_dir)

    lock_file = work_dir + "/MGF_Offset.lock"
    if os.path.isfile(lock_file):
        logger.get_root_logger().info("Lock file exists:" + os.path.abspath(lock_file) + " program stopped.")
        return
    logfile = work_dir + "/" + "MGF_Offset_" + date_time_tag.get_tag() + ".log"

    console_logger.add_filelogger(level.DEBUG, logfile)
    logger.get_root_logger().info("Creating lock file:" + os.path.abspath(lock_file))
    Path(lock_file).touch()

    try:
        deletelogfile_dia = update_dia_mgf_offset()
        deletelogfile_dda = update_dda_mgf_offset()
        deletelogfile_hcd = update_hcd_mgf_offset()
        deletelogfile_etd = update_etd_mgf_offset()
        logger.get_root_logger().info("Deleting lock file:" + os.path.abspath(lock_file))
        os.remove(lock_file)

        if deletelogfile_dda and deletelogfile_dia and deletelogfile_hcd and deletelogfile_etd:
            os.remove(logfile)
    except:
        logger.get_root_logger().error("MGF Offset failed!")

def update_etd_mgf_offset():
    mgf_folder = PathFinder().get_etd_mgf_folder()

    deletelog = True
    executor_pool = executors.new_fixed_thread_pool(8)
    for dir in os.listdir(mgf_folder):
        dir = mgf_folder + dir
        if os.path.isdir(dir):
            for mgf in os.listdir(dir):
                mgf = dir + mgf
                if os.path.isfile(mgf):
                    basename = os.path.basename(mgf)
                    if not os.path.isfile(work_dir + "/" + basename + ".done") and not os.path.isfile(work_dir + "/" + basename + ".error") and not os.path.isfile(work_dir + "/" + basename + ".process"):
                        if (database_ms.get_ms_run_id(basename) == -1):
                            continue
                        if (database_ms.get_no_ms2_in_db(basename) == 0):
                            continue
                        deletelog = False
                        unit = UpdateMgfOffsetUnit(mgf, True, work_dir, db_connection_manager)
                        executor_pool.execute(unit)
    executor_pool.shutdown()
    try:
        executor_pool.await_termination(sys.maxint)
    except KeyboardInterrupt:
        print("interrupted..")
    return deletelog

def update_hcd_mgf_offset():
    mgf_folder = PathFinder().get_hcd_mgf_folder()

    deletelog = True
    executor_pool = executors.new_fixed_thread_pool(8)
    for dir in os.listdir(mgf_folder):
        dir = mgf_folder + dir
        if os.path.isdir(dir):
            for mgf in os.listdir(dir):
                mgf = dir + mgf
                if os.path.isfile(mgf):
                    basename = os.path.basename(mgf)
                    if not os.path.isfile(work_dir + "/" + basename + ".done") and not os.path.isfile(work_dir + "/" + basename + ".error") and not os.path.isfile(work_dir + "/" + basename + ".process"):
                        if (database_ms.get_ms_run_id(basename) == -1):
                            continue
                        if (database_ms.get_no_ms2_in_db(basename) == 0):
                            continue
                        deletelog = False
                        unit = UpdateMgfOffsetUnit(mgf, True, work_dir, db_connection_manager)
                        executor_pool.execute(unit)
    executor_pool.shutdown()
    try:
        executor_pool.await_termination(sys.maxint)
    except KeyboardInterrupt:
        print("interrupted..")
    return deletelog

def update_dda_mgf_offset():
    mgf_folder = PathFinder().get_dda_mgf_folder()

    deletelog = True
    executor_pool = executors.new_fixed_thread_pool(8)
    for dir in os.listdir(mgf_folder):
        dir = mgf_folder + dir
        if os.path.isdir(dir):
            for mgf in os.listdir(dir):
                mgf = dir + mgf
                if os.path.isfile(mgf):
                    basename = os.path.basename(mgf)
                    if not os.path.isfile(work_dir + "/" + basename + ".done") and not os.path.isfile(work_dir + "/" + basename + ".error") and not os.path.isfile(work_dir + "/" + basename + ".process"):
                        if (database_ms.get_ms_run_id(basename) == -1):
                            continue
                        if (database_ms.get_no_ms2_in_db(basename) == 0):
                            continue
                        deletelog = False
                        unit = UpdateMgfOffsetUnit(mgf, True, work_dir, db_connection_manager)
                        executor_pool.execute(unit)
    executor_pool.shutdown()
    try:
        executor_pool.await_termination(sys.maxint)
    except KeyboardInterrupt:
        print("interrupted..")
    return deletelog

def update_dia_mgf_offset():
    mgf_folder = PathFinder().get_dia_mgf_folder()

    deletelog = True
    executor_pool = executors.new_fixed_thread_pool(8)
    for mgf in os.listdir(mgf_folder):
        mgf = mgf_folder + mgf
        if ".pMS2.mgf" in mgf:
            basename = os.path.basename(mgf).replace(".pMS2", "")
            if not os.path.isfile(work_dir + "/" + basename + ".done") and not os.path.isfile(work_dir + "/" + basename + ".error") and not os.path.isfile(work_dir + "/" + basename + ".process"):
                if (database_ms.get_ms_run_id(basename) == -1):
                    continue
                deletelog = False
                unit = UpdateMgfOffsetUnit(mgf, False, work_dir, db_connection_manager)
                executor_pool.execute(unit)
    executor_pool.shutdown()
    try:
        executor_pool.await_termination(sys.maxint)
    except KeyboardInterrupt:
        print("interrupted..")
    return deletelog

# call the main function
if __name__ == '__main__':
    main()
