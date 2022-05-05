import atexit
import os
import signal
import platform
import tempfile
import shutil
import time
from subprocess import Popen, PIPE
import struct

from py4j.java_gateway import java_import, JavaGateway, JavaObject, GatewayParameters
from py4j.clientserver import ClientServer, JavaParameters, PythonParameters


def read_int(stream):
    length = stream.read(4)
    if not length:
        raise EOFError
    return struct.unpack("!i", length)[0]

def test():
    on_windows = platform.system() == "Windows"
    command = ["java", "-classpath", r"D:\IdeaWorkspace\java-utils\spark-utils\target\spark-utils-jar-with-dependencies.jar",
               "com.util.python.PythonGatewayServer"]

    conn_info_dir = tempfile.mkdtemp()
    try:
        fd, conn_info_file = tempfile.mkstemp(dir=conn_info_dir)
        os.close(fd)
        os.unlink(conn_info_file)

        env = dict(os.environ)
        env["_PYSPARK_DRIVER_CONN_INFO_PATH"] = conn_info_file

        # Launch the Java gateway.
        popen_kwargs = {}
        # We open a pipe to stdin so that the Java gateway can die when the pipe is broken
        popen_kwargs['stdin'] = PIPE
        # We always set the necessary environment variables.
        popen_kwargs['env'] = env
        if not on_windows:
            # Don't send ctrl-c / SIGINT to the Java gateway:
            def preexec_func():
                signal.signal(signal.SIGINT, signal.SIG_IGN)

            popen_kwargs['preexec_fn'] = preexec_func
            # 启动spark-submit字进程
            proc = Popen(command, **popen_kwargs)
        else:
            # preexec_fn not supported on Windows
            proc = Popen(command, **popen_kwargs)

        # Wait for the file to appear, or for the process to exit, whichever happens first.
        while not proc.poll() and not os.path.isfile(conn_info_file):
            time.sleep(0.1)

        if not os.path.isfile(conn_info_file):
            raise Exception("Java gateway process exited before sending its port number")

        # conn_info_file文件中有和java进程通信的信息
        with open(conn_info_file, "rb") as info:
            gateway_port = read_int(info)

    finally:
        shutil.rmtree(conn_info_dir)

    # In Windows, ensure the Java child processes do not linger after Python has exited.
    # In UNIX-based systems, the child process can kill itself on broken pipe (i.e. when
    # the parent process' stdin sends an EOF). In Windows, however, this is not possible
    # because java.lang.Process reads directly from the parent process' stdin, contending
    # with any opportunity to read an EOF from the parent. Note that this is only best
    # effort and will not take effect if the python process is violently terminated.
    if on_windows:
        # In Windows, the child process here is "spark-submit.cmd", not the JVM itself
        # (because the UNIX "exec" command is not available). This means we cannot simply
        # call proc.kill(), which kills only the "spark-submit.cmd" process but not the
        # JVMs. Instead, we use "taskkill" with the tree-kill option "/t" to terminate all
        # child processes in the tree (http://technet.microsoft.com/en-us/library/bb491009.aspx)
        def killChild():
            pass
            Popen(["cmd", "/c", "taskkill", "/f", "/t", "/pid", str(proc.pid)])
        atexit.register(killChild)

    # 创建gateway, py4j提供的类
    gateway = JavaGateway(
        gateway_parameters=GatewayParameters(
            port=gateway_port,
            auto_convert=True))

    # Store a reference to the Popen object for use by the caller (e.g., in reading stdout/stderr)
    gateway.proc = proc

    javaUtils = gateway.jvm.com.util.Utils
    print(javaUtils.stringToSeq("1,2,3,4"))


if __name__ == '__main__':
    test()