import re
from collections import defaultdict
import time
from urllib.parse import urlparse
import glob
import os

def get_main_domain(url):
    """Extract main domain from URL"""
    parsed = urlparse(url)
    domain = parsed.netloc.lower()
    
    # Map to main domains
    if 'ics.uci.edu' in domain:
        return 'ics.uci.edu'
    elif 'cs.uci.edu' in domain:
        return 'cs.uci.edu'
    elif 'informatics.uci.edu' in domain:
        return 'informatics.uci.edu'
    elif 'stat.uci.edu' in domain:
        return 'stat.uci.edu'
    return domain

def analyze_logs(log_files):
    """Analyze multiple crawler log files for politeness"""
    # Track last access time for each main domain
    domain_last_access = defaultdict(float)
    # Track violations
    violations = defaultdict(list)
    # Track statistics
    stats = {
        'total_requests': defaultdict(int),
        'avg_delay': defaultdict(list),
        'min_delay': defaultdict(float),
        'max_delay': defaultdict(float)
    }
    
    # Regular expression to match log entries
    log_pattern = re.compile(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) - Worker-\d+ - INFO - Downloaded (https?://[^\s,]+)')
    
    for log_file in log_files:
        print(f"\nAnalyzing {log_file}...")
        with open(log_file, 'r') as f:
            for line in f:
                match = log_pattern.search(line)
                if match:
                    timestamp_str, url = match.groups()
                    timestamp = time.mktime(time.strptime(timestamp_str.split(',')[0], '%Y-%m-%d %H:%M:%S'))
                    main_domain = get_main_domain(url)
                    
                    stats['total_requests'][main_domain] += 1
                    
                    # Check time since last access
                    if domain_last_access[main_domain]:
                        delay = timestamp - domain_last_access[main_domain]
                        stats['avg_delay'][main_domain].append(delay)
                        
                        if stats['min_delay'][main_domain] == 0 or delay < stats['min_delay'][main_domain]:
                            stats['min_delay'][main_domain] = delay
                            
                        if delay > stats['max_delay'][main_domain]:
                            stats['max_delay'][main_domain] = delay
                            
                        if delay < 0.5:  # Less than 500ms
                            violations[main_domain].append({
                                'url': url,
                                'timestamp': timestamp_str,
                                'delay': delay,
                                'log_file': os.path.basename(log_file)
                            })
                    
                    domain_last_access[main_domain] = timestamp
    
    # Print analysis
    print("\nPoliteness Analysis Report")
    print("=" * 80)
    
    # Print statistics for each domain
    print("\nDomain Statistics:")
    print("-" * 80)
    for domain in sorted(stats['total_requests'].keys()):
        avg_delay = sum(stats['avg_delay'][domain]) / len(stats['avg_delay'][domain]) if stats['avg_delay'][domain] else 0
        print(f"\n{domain}:")
        print(f"  Total Requests: {stats['total_requests'][domain]}")
        print(f"  Average Delay: {avg_delay:.3f}s")
        print(f"  Minimum Delay: {stats['min_delay'][domain]:.3f}s")
        print(f"  Maximum Delay: {stats['max_delay'][domain]:.3f}s")
    
    if not violations:
        print("\n✅ No politeness violations found!")
        return
    
    print("\n❌ Politeness Violations:")
    print("-" * 80)
    for domain, domain_violations in sorted(violations.items()):
        print(f"\n{domain}:")
        print(f"Total violations: {len(domain_violations)}")
        print("Sample violations:")
        for v in domain_violations[:5]:  # Show first 5 violations
            print(f"  {v['timestamp']} - {v['url']}")
            print(f"    Delay: {v['delay']:.3f}s (in {v['log_file']})")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python check_politeness.py <log_file_pattern>")
        print("Example: python check_politeness.py 'crawler*.log'")
        sys.exit(1)
    
    log_files = glob.glob(sys.argv[1])
    if not log_files:
        print(f"No log files found matching pattern: {sys.argv[1]}")
        sys.exit(1)
        
    analyze_logs(log_files) 