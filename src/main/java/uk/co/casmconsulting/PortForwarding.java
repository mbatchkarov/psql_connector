package uk.co.casmconsulting; /**
 * Created by mmb28 on 14/07/2016.
 * <p>
 * This program will demonstrate the port forwarding like option -L of
 * ssh command; the given port on the local host will be forwarded to
 * the given remote host and port on the remote side.
 * Authentication is done with client's private key and server's public key,
 * both stored in a file. If everything works fine, you will get the shell
 * prompt. Try the port on localhost.
 */

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;


public class PortForwarding {
    private int lport = 4321;
    private String rhost = "127.0.0.1";
    private int rport = 5432;
    private String userKeyFile = "privatekey";
    private String serverKeyFile = "publickey-casm-server";

    private Session session;

    public void start(String user, String host) {
        try {
            JSch jsch = new JSch();

            // copy private key to a temp file because jsch insist on reading from a file
            InputStream ddlStream = getClass().getResourceAsStream("/" + userKeyFile);
            File temp = File.createTempFile("temp-prvkey", ".tmp");
            temp.deleteOnExit();

            try (FileOutputStream fos = new FileOutputStream(temp);) {
                byte[] buf = new byte[2048];
                int r;
                while (-1 != (r = ddlStream.read(buf))) {
                    fos.write(buf, 0, r);
                }
            }

            jsch.addIdentity(temp.getAbsolutePath());
            jsch.setKnownHosts(getClass().getResourceAsStream("/" + serverKeyFile));
            temp.delete();
            session = jsch.getSession(user, host, 22);
            session.connect();
            int assignedPort = session.setPortForwardingL(lport, rhost, rport);
            System.out.println("Tunneling localhost:" + assignedPort + " -> " + rhost + ":" + rport);
        } catch (Exception e) {
            System.out.println(e);
            e.printStackTrace();
        }

    }

    public void stop() {
        session.disconnect();
    }

}