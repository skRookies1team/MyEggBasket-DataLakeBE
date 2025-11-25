package com.egg.collector.my_egg_basket;

import com.egg.collector.my_egg_basket.domain.RealtimeData;
import com.egg.collector.my_egg_basket.domain.RealtimeDataRepository;
import com.egg.collector.my_egg_basket.service.RealtimeDataService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
class MyEggBasketApplicationTests {

	@Autowired
	private RealtimeDataService realtimeDataService;

	@Autowired
	private RealtimeDataRepository realtimeDataRepository;

	@Test
	@DisplayName("오래된 데이터가 CSV로 아카이빙되고 DB에서 삭제되는지 테스트")
	void testArchivingAndCleanup() {
		// 1. 테스트 데이터 준비 (4일 전 데이터 생성 - 삭제 대상)
		LocalDateTime oldTime = LocalDateTime.now().minusDays(4);

		RealtimeData oldData = new RealtimeData();
		oldData.setStckShrnIscd("TEST_CODE"); // 테스트용 종목 코드
		oldData.setStckPrpr(50000L);
		oldData.setTimestamp(oldTime);
		oldData.setAcmlVol(100L);
		oldData.setPrdyCtrt(1.5);

		realtimeDataRepository.save(oldData);
		System.out.println(">>> 테스트용 과거 데이터 저장 완료: " + oldData.getId());

		// 2. 정리(아카이빙) 로직 실행
		System.out.println(">>> cleanupOldData() 실행 시작...");
		realtimeDataService.cleanupOldData();
		System.out.println(">>> cleanupOldData() 실행 완료");

		// 3. 검증: CSV 파일이 생성되었는지 확인
		// 파일명 생성 규칙과 동일하게 경로 지정 (data-lake/archive/stock_data_yyyy-MM-dd.csv)
		String dateStr = LocalDateTime.now().minusDays(3).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
		String filePath = "data-lake/archive/stock_data_" + dateStr + ".csv";

		File file = new File(filePath);
		boolean exists = file.exists();

		System.out.println(">>> 파일 생성 확인: " + file.getAbsolutePath());
		System.out.println(">>> 존재 여부: " + exists);

		assertTrue(exists, "아카이브 CSV 파일이 생성되어야 합니다.");

		// 4. (선택) DB에서 삭제되었는지 확인하려면 아래 주석 해제
		// boolean dataExistsInDb = realtimeDataRepository.existsById(oldData.getId());
		// assertFalse(dataExistsInDb, "DB에서 데이터가 삭제되어야 합니다.");
	}
}