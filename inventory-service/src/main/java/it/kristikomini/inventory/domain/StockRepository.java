package it.kristikomini.inventory.domain;

import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;

public interface StockRepository extends JpaRepository<Stock, Long> {

    /** Plain read — used by the optimistic strategy (conflict detected via @Version on save). */
    Optional<Stock> findBySku(String sku);

    /** SELECT … FOR UPDATE — used by the pessimistic strategy; concurrent reservers serialise here. */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT s FROM Stock s WHERE s.sku = :sku")
    Optional<Stock> lockBySku(@Param("sku") String sku);
}
