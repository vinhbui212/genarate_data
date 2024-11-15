package org.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Random;

public class MultithreadedInsert {

    private static final String DB_URL = "jdbc:mysql://localhost:3306/demo";
    private static final String USER = "root";
    private static final String PASSWORD = "1234";
    private static final int TOTAL_RECORDS = 5_000_000;
    private static final int THREAD_COUNT = 10;

    public static void main(String[] args) {
        int recordsPerThread = TOTAL_RECORDS / THREAD_COUNT;

        for (int i = 0; i < THREAD_COUNT; i++) {
            int startId = i * recordsPerThread + 1;
            int endId = (i + 1) * recordsPerThread;
            Thread thread = new Thread(new InsertTask(startId, endId));
            thread.start();
        }
    }

    static class InsertTask implements Runnable {
        private int startId;
        private int endId;

        public InsertTask(int startId, int endId) {
            this.startId = startId;
            this.endId = endId;
        }

        @Override
        public void run() {
            try (Connection connection = DriverManager.getConnection(DB_URL, USER, PASSWORD)) {
                connection.setAutoCommit(false);
//                String sql = "INSERT INTO customer (pk_customer_id, name, uk_mail) VALUES (?, ?, ?)";
                String sql = "UPDATE customer SET parent_id = ? WHERE pk_customer_id = ?";

                PreparedStatement preparedStatement = connection.prepareStatement(sql);
                Random random = new Random();
                int batchSize = 1000;

                for (int i = startId; i <= endId; i++) {
                    int parentId = random.nextInt(2);
                    preparedStatement.setInt(1, parentId);
                    preparedStatement.setInt(2, i);
                    preparedStatement.addBatch();

                    if ((i - startId + 1) % batchSize == 0) {
                        preparedStatement.executeBatch();
                        connection.commit();
                        System.out.println("Thread " + Thread.currentThread().getId() + " đã chèn " + (i - startId + 1) + " bản ghi.");
                    }
                }
                preparedStatement.executeBatch();
                connection.commit();
                System.out.println("Thread " + Thread.currentThread().getId() + " đã hoàn tất chèn từ " + startId + " đến " + endId + ".");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
