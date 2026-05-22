package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class TransactionListener {

    static final Logger logger = LoggerFactory.getLogger(TransactionListener.class);

    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRecordRepository;
    private final RestTemplate restTemplate;

    public TransactionListener(UserRepository userRepository,
            TransactionRecordRepository transactionRecordRepository,
            RestTemplate restTemplate) {
        this.userRepository = userRepository;
        this.transactionRecordRepository = transactionRecordRepository;
        this.restTemplate = restTemplate;
    }

    @KafkaListener(topics = "${general.kafka-topic}")
    public void listen(Transaction transaction) {

        // 1. Validate sender aur recipient
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        if (sender == null || recipient == null) {
            logger.warn("Invalid sender or recipient — discarding");
            return;
        }

        // 2. Balance check karo
        if (sender.getBalance() < transaction.getAmount()) {
            logger.warn("Insufficient balance — discarding");
            return;
        }

        // 3. Incentive API call karo
        float incentiveAmount = 0;
        try {
            Incentive incentive = restTemplate.postForObject(
                    "http://localhost:8080/incentive",
                    transaction,
                    Incentive.class);
            if (incentive != null) {
                incentiveAmount = incentive.getAmount();
            }
        } catch (Exception e) {
            logger.warn("Incentive API call failed: {}", e.getMessage());
        }

        // 4. Balances update karo
        // Sender se amount deduct karo
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        // Recipient ko amount + incentive do
        recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);
        userRepository.save(sender);
        userRepository.save(recipient);

        // 5. Transaction record karo
        transactionRecordRepository.save(
                new TransactionRecord(sender, recipient, transaction.getAmount(), incentiveAmount));

        logger.info("Saved: {} -> {} amount={} incentive={}",
                sender.getName(), recipient.getName(),
                transaction.getAmount(), incentiveAmount);
    }
}