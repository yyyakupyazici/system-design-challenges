package io.paramix.redislab.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.paramix.redislab.leaderboard.LeaderboardService;
import io.paramix.redislab.leaderboard.LeaderboardService.Entry;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class LeaderboardIT extends AbstractRedisIT {

    @Autowired LeaderboardService lb;

    @Test
    void topRankAndAroundMe() {
        String board = unique("haftalik");
        lb.addScore(board, "ali", 120);
        lb.addScore(board, "ayse", 80);
        lb.addScore(board, "mehmet", 200);
        lb.addScore(board, "zeynep", 95);
        lb.addScore(board, "ayse", 50);                        // ZINCRBY: 80 → 130, ali'nin önüne geçer

        assertThat(lb.top(board, 3)).containsExactly(
                new Entry("mehmet", 200, 1), new Entry("ayse", 130, 2), new Entry("ali", 120, 3));
        assertThat(lb.rank(board, "zeynep")).isEqualTo(4);
        assertThat(lb.rank(board, "yok")).isEqualTo(-1);
        assertThat(lb.around(board, "ali", 1)).extracting(Entry::member).containsExactly("ayse", "ali", "zeynep");
    }
}
