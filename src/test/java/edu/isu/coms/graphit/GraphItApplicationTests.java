package edu.isu.coms.graphit;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = GraphItApplication.class)
@WebAppConfiguration
public class GraphItApplicationTests {

	@Test
	public void contextLoads() {
	}

}
