package com.github.stuxuhai.hdata.logger;

import java.io.IOException;
import java.io.OutputStream;

/**
 * The set of console output targets.

 * @author Ruediger Dohna
 * @author Ceki G&uuml;lc&uuml;
 * @author Tom SH Liu
 * @author David Roussel
 */
public enum ConsoleTarget {

    SystemOut("System.out", new OutputStream() {
        @Override
        public void write(int b) throws IOException {
            System.out.write(b);
        }

        @Override
        public void write(byte b[]) throws IOException {
            System.out.write(b);
        }

        @Override
        public void write(byte b[], int off, int len) throws IOException {
            System.out.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            System.out.flush();
        }
    }),

    SystemErr("System.err", new OutputStream() {
        @Override
        public void write(int b) throws IOException {
            System.err.write(b);
        }

        @Override
        public void write(byte b[]) throws IOException {
            System.err.write(b);
        }

        @Override
        public void write(byte b[], int off, int len) throws IOException {
            System.err.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            System.err.flush();
        }
    });

    public static ConsoleTarget findByName(String name) {
        for (ConsoleTarget target : ConsoleTarget.values()) {
            if (target.name.equalsIgnoreCase(name)) {
                return target;
            }
        }
        return null;
    }

    private final String name;
    private final OutputStream stream;

    private ConsoleTarget(String name, OutputStream stream) {
        this.name = name;
        this.stream = stream;
    }

    public String getName() {
        return name;
    }

    public OutputStream getStream() {
        return stream;
    }

    @Override
    public String toString() {
        return name;
    }
}