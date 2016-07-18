package uk.co.casmconsulting;

import javax.swing.*;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Created by mmb28 on 15/07/2016.
 */
public class JTextAreaOutputStream extends OutputStream {
    private JTextArea textArea;

    public JTextAreaOutputStream(JTextArea textArea) {
        this.textArea = textArea;
    }

    @Override
    public void write(int b) throws IOException {
        // redirects data to the text area
        textArea.append(String.valueOf((char) b));
        // scrolls the text area to the end of data
        textArea.setCaretPosition(textArea.getDocument().getLength());
    }
}
