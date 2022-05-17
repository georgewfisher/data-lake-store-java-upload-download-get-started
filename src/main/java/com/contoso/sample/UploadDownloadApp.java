package com.contoso.sample;

import com.microsoft.azure.datalake.store.ADLException;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.DirectoryEntry;
import com.microsoft.azure.datalake.store.IfExists;
import com.microsoft.azure.datalake.store.oauth2.AccessTokenProvider;
import com.microsoft.azure.datalake.store.oauth2.ClientCredsTokenProvider;
import com.microsoft.azure.datalake.store.oauth2.MsiTokenProvider;

import java.io.*;
import java.util.Arrays;
import java.util.List;

public class UploadDownloadApp {

    // This needs to be filled in for the app to work
    private static String accountFQDN = "fqdn";  // full account FQDN, not just the account name
    private static String basePath = "/base/path";
    private static boolean deleteAfter = false;

    public static void main(String[] args) {
        try {
            System.out.println("Establishing connection...");
            // Create client object using MSI creds
            AccessTokenProvider provider = new MsiTokenProvider();
            ADLStoreClient client = ADLStoreClient.createClient(accountFQDN, provider);

            // Enumerate base directory (test reading before writing)
            System.out.println("Reading directory...");
            List<DirectoryEntry> directoryEntries = client.enumerateDirectory(basePath);
            System.out.println("Directory " + basePath + " contains " + directoryEntries.size() + " items.");
            for (DirectoryEntry de : directoryEntries) {
                printDirectoryInfo(de);
            }

            System.out.println("Create directory and file...");

            // create directory
            client.createDirectory(basePath + "/a/b/w");

            // create file and write some content
            String filename = basePath + "/a/b/c.txt";
            OutputStream stream = client.createFile(filename, IfExists.OVERWRITE  );
            PrintStream out = new PrintStream(stream);
            for (int i = 1; i <= 10; i++) {
                out.println("This is line #" + i);
                out.format("This is the same line (%d), but using formatted output. %n", i);
            }
            out.close();

            System.out.println("Set file permissions...");
            // set file permission
            client.setPermission(filename, "744");

            System.out.println("Append...");

            // append to file
            stream = client.getAppendStream(filename);
            stream.write(getSampleContent());
            stream.close();

            System.out.println("Read back files...");

            // Read File
            InputStream in = client.getReadStream(filename);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String line;
            while ( (line = reader.readLine()) != null) {
                System.out.println(line);
            }
            reader.close();
            System.out.println();

            System.out.println("Inspect metadata...");

            // get file metadata
            DirectoryEntry ent = client.getDirectoryEntry(filename);
            printDirectoryInfo(ent);

            System.out.println("Create file with byte array...");

            // create another file - this time using a byte array
            stream = client.createFile(basePath + "/a/b/d.txt", IfExists.OVERWRITE);
            byte[] buf = getSampleContent();
            stream.write(buf);
            stream.close();

            System.out.println("Concat two files...");

            // concatenate the two files into one
            List<String> fileList = Arrays.asList(basePath + "/a/b/c.txt", basePath + "/a/b/d.txt");
            client.concatenateFiles(basePath + "/a/b/f.txt", fileList);

            //rename the file
            System.out.println("Rename files...");
            client.rename(basePath + "/a/b/f.txt", basePath + "/a/b/g.txt");

            if (deleteAfter) {
                // delete directory along with all the subdirectories and files in it
                System.out.println("Delete files and directories...");
                client.deleteRecursive(basePath + "/a");
            }

        } catch (ADLException ex) {
            printExceptionDetails(ex);
        } catch (Exception ex) {
            System.out.format(" Exception: %s%n Message: %s%n", ex.getClass().getName(), ex.getMessage());
        }
    }

    private static void printExceptionDetails(ADLException ex) {
        System.out.println("ADLException:");
        System.out.format("  Message: %s%n", ex.getMessage());
        System.out.format("  HTTP Response code: %s%n", ex.httpResponseCode);
        System.out.format("  Remote Exception Name: %s%n", ex.remoteExceptionName);
        System.out.format("  Remote Exception Message: %s%n", ex.remoteExceptionMessage);
        System.out.format("  Server Request ID: %s%n", ex.requestId);
        System.out.println();
    }

    private static void printDirectoryInfo(DirectoryEntry ent) {
        System.out.format("Name: %s%n", ent.name);
        System.out.format("  FullName: %s%n", ent.fullName);
        System.out.format("  Length: %d%n", ent.length);
        System.out.format("  Type: %s%n", ent.type.toString());
        System.out.format("  Group: %s%n", ent.group);
        System.out.format("  User: %s%n", ent.user);
        System.out.format("  Permission: %s%n", ent.permission);
        System.out.format("  mtime: %s%n", ent.lastModifiedTime.toString());
        System.out.format("  atime: %s%n", ent.lastAccessTime.toString());
        System.out.println();
    }

    private static byte[] getSampleContent() {
        ByteArrayOutputStream s = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(s);
        out.println("This is a line");
        out.println("This is another line");
        out.println("This is yet another line");
        out.println("This is yet yet another line");
        out.println("This is yet yet yet another line");
        out.println("... and so on, ad infinitum");
        out.println();
        out.close();
        return s.toByteArray();
    }
}
