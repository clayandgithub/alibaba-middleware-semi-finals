package com.alibaba.middleware.race.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.TreeMap;

public class FileUtil {

    public static final char EMPTY_CHAR = '\0';

    public static void createDir(final String absDirName) {
        final File dir = new File(absDirName);
        if (!dir.exists() || !dir.isDirectory()) {
            dir.mkdir();
        }
    }

    public static void writeFixedBytesLineWithFile(RandomAccessFile openedFile,
            final String encoding, final String content, final int bytesOfLine,
            final long lineNo) {
        try {
            /**
             * insert emptyLine if necessary
             */
            {
                long fileBytesNum = openedFile.length();
                long fileLineNum = fileBytesNum / bytesOfLine;
                if (fileLineNum < lineNo) {
                    StringBuilder emptyLine = new StringBuilder();
                    for (int i = 0; i < bytesOfLine - 1; ++i) {
                        emptyLine.append(EMPTY_CHAR);
                    }
                    emptyLine.append("\n");
                    long needInsertLineNum = lineNo - fileLineNum;
                    for (long i = 0; i < needInsertLineNum; ++i) {
                        openedFile.seek((fileLineNum + i) * bytesOfLine);
                        openedFile.write(emptyLine.toString()
                                .getBytes(encoding));
                    }
                }
            }

            StringBuilder sb = new StringBuilder(content);
            {
                /**
                 * construct content to bytesOfLine
                 */
                int originalBytes = content.getBytes(encoding).length;
                if (originalBytes > bytesOfLine - 1) {
                    throw new IOException(content
                            + " : originalBytes > bytesOfLine - 1");
                }
                for (int i = originalBytes; i < bytesOfLine - 1; ++i) {
                    sb.append(EMPTY_CHAR);
                }
                sb.append("\n");
            }

            long startByteNo = lineNo * bytesOfLine;
            openedFile.seek(startByteNo);
            openedFile.write(sb.toString().getBytes(encoding));
        } catch (final IOException e) {
            e.printStackTrace();
        }
    }

    public static String getFixedBytesLineWithFile(RandomAccessFile openedFile,
            final String encoding, final int bytesOfLine, final int lineNo,
            final boolean removeEmptyChar) {
        String line = null;
        try {

            long startByteNo = lineNo * bytesOfLine;
            openedFile.seek(startByteNo);
            byte[] lineBuffer = new byte[bytesOfLine];
            openedFile.read(lineBuffer);
            if (removeEmptyChar) {
                int firstEmptyCharIdx = -1;
                for (int i = 0; i < lineBuffer.length; ++i) {
                    if (lineBuffer[i] == EMPTY_CHAR) {
                        firstEmptyCharIdx = i;
                        break;
                    }
                }
                if (firstEmptyCharIdx > 0) {
                    line = new String(lineBuffer, 0, firstEmptyCharIdx,
                            encoding);
                } else {
                    line = new String(lineBuffer, 0, lineBuffer.length - 1,
                            encoding);
                }

            } else {
                line = new String(lineBuffer, 0, lineBuffer.length - 1,
                        encoding);
            }
        } catch (final IOException e) {
            e.printStackTrace();
        }
        return line;
    }

    public static void writeFixedBytesLine(final String filePath,
            final String encoding, final String content, final int bytesOfLine,
            final long lineNo) {
        RandomAccessFile rf = null;
        try {
            rf = new RandomAccessFile(filePath, "rw");
            /**
             * insert emptyLine if necessary
             */
            {
                long fileBytesNum = rf.length();
                long fileLineNum = fileBytesNum / bytesOfLine;
                if (fileLineNum < lineNo) {
                    StringBuilder emptyLine = new StringBuilder();
                    for (int i = 0; i < bytesOfLine - 1; ++i) {
                        emptyLine.append(EMPTY_CHAR);
                    }
                    emptyLine.append("\n");
                    long needInsertLineNum = lineNo - fileLineNum;
                    for (long i = 0; i < needInsertLineNum; ++i) {
                        rf.seek((fileLineNum + i) * bytesOfLine);
                        rf.write(emptyLine.toString().getBytes(encoding));
                    }
                }
            }

            StringBuilder sb = new StringBuilder(content);
            {
                /**
                 * construct content to bytesOfLine
                 */
                int originalBytes = content.getBytes(encoding).length;
                if (originalBytes > bytesOfLine - 1) {
                    throw new IOException(content
                            + " : originalBytes > bytesOfLine - 1");
                }
                for (int i = originalBytes; i < bytesOfLine - 1; ++i) {
                    sb.append(EMPTY_CHAR);
                }
                sb.append("\n");
            }

            long startByteNo = lineNo * bytesOfLine;
            rf.seek(startByteNo);
            rf.write(sb.toString().getBytes(encoding));
        } catch (final IOException e) {
            e.printStackTrace();
        } finally {
            try {
                rf.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static String getFixedBytesLine(final String filePath,
            final String encoding, final int bytesOfLine, final long lineNo,
            final boolean removeEmptyChar) {
        RandomAccessFile rf = null;
        String line = null;
        try {
            rf = new RandomAccessFile(filePath, "r");

            long startByteNo = lineNo * bytesOfLine;
            rf.seek(startByteNo);
            byte[] lineBuffer = new byte[bytesOfLine];
            rf.read(lineBuffer);
            if (removeEmptyChar) {
                int firstEmptyCharIdx = -1;
                for (int i = 0; i < lineBuffer.length; ++i) {
                    if (lineBuffer[i] == EMPTY_CHAR) {
                        firstEmptyCharIdx = i;
                        break;
                    }
                }
                if (firstEmptyCharIdx >= 0) {
                    line = new String(lineBuffer, 0, firstEmptyCharIdx,
                            encoding);
                } else {
                    line = new String(lineBuffer, 0, lineBuffer.length - 1,
                            encoding);
                }

            } else {
                line = new String(lineBuffer, 0, lineBuffer.length - 1,
                        encoding);
            }
        } catch (final IOException e) {
            e.printStackTrace();
        } finally {
            try {
                rf.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return line;
    }

    public static boolean isFileExist(final String absFileName) {
        final File file = new File(absFileName);
        if (file.exists()) {
            return true;
        }
        return false;
    }

    public static boolean deleteFileIfExist(final String absFileName) {
        final File file = new File(absFileName);
        if (file.exists()) {
            return file.delete();
        }
        return true;
    }

    /**
     * @param regionFile
     * @param encoding
     * @param content
     * @return
     */
    public static long appendLineWithRandomAccessFile(
            RandomAccessFile openedFile, String encoding, String content) {
        long length = 0;
        try {
            content = content.concat("\n");
            long fileBytesNum = openedFile.length();
            openedFile.seek(fileBytesNum);
            byte[] bytes = content.toString().getBytes(encoding);
            length = bytes.length;
            openedFile.write(bytes);
        } catch (final IOException e) {
            e.printStackTrace();
        }
        return length;
    }

    public static long appendFixedBytesLineWithRandomAccessFile(
            RandomAccessFile openedFile, String encoding, String content,
            int bytesOfLine) {
        long length = 0;
        try {
            StringBuilder sb = new StringBuilder(content);
            {
                /**
                 * construct content to bytesOfLine
                 */
                int originalBytes = content.getBytes(encoding).length;
                if (originalBytes > bytesOfLine - 1) {
                    throw new IOException(content
                            + " : originalBytes > bytesOfLine - 1");
                }
                for (int i = originalBytes; i < bytesOfLine - 1; ++i) {
                    sb.append(EMPTY_CHAR);
                }
                sb.append("\n");
            }
            long fileBytesNum = openedFile.length();
            openedFile.seek(fileBytesNum);
            byte[] bytes = sb.toString().getBytes(encoding);
            length = bytes.length;
            openedFile.write(bytes);
        } catch (final IOException e) {
            e.printStackTrace();
        }
        return length;
    }

    public static HashMap<String, Integer> readSIHashMapFromFile(
            String pathname, int initialCapacity) {
        HashMap<String, Integer> retMap = new HashMap<String, Integer>(
                initialCapacity);
        BufferedReader bufferedReader;
        try {
            bufferedReader = new BufferedReader(new FileReader(pathname));
            String line = null;
            try {
                while ((line = bufferedReader.readLine()) != null) {
                    String[] splitOfLine = line.split(":");
                    if (splitOfLine.length == 2) {
                        retMap.put(splitOfLine[0].trim(),
                                Integer.parseInt(splitOfLine[1].trim()));
                    } else {
                        throw new IOException("This line is not valid! : "
                                + line);
                    }
                }
            } finally {
                bufferedReader.close();
            }
        } catch (final IOException e) {
            e.printStackTrace();
        }
        return retMap;
    }

    public static TreeMap<String, String> readSSTreeMapFromFile(String pathname) {
        TreeMap<String, String> retMap = new TreeMap<String, String>();
        BufferedReader bufferedReader;
        try {
            bufferedReader = new BufferedReader(new FileReader(pathname));
            String line = null;
            try {
                while ((line = bufferedReader.readLine()) != null) {
                    String[] splitOfLine = line.split(":");
                    if (splitOfLine.length == 2) {
                        retMap.put(splitOfLine[0].trim(), splitOfLine[1].trim());
                    } else {
                        throw new IOException("This line is not valid! : "
                                + line);
                    }
                }
            } finally {
                bufferedReader.close();
            }
        } catch (final IOException e) {
            e.printStackTrace();
        }
        return retMap;
    }
}
