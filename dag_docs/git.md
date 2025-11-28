# Git 기본 명령어 모음


## 1️⃣ 로컬 저장소 설정

```bash
git init                  # 새 로컬 저장소 생성
git clone <repo_url>      # 원격 저장소 복제
```

## 2️⃣ 상태 확인

```bash
git status                # 현재 변경사항 확인
git log                   # 커밋 히스토리 확인
git log --oneline --graph # 간단한 그래프 형태로 히스토리 확인
git diff                  # 변경된 내용 확인
```

## 3️⃣ 파일 관리

```bash
git add <file>            # 파일 스테이징
git add .                 # 모든 변경 파일 스테이징
git rm <file>             # 파일 삭제 후 스테이징
git mv <old> <new>        # 파일 이름 변경
```

## 4️⃣ 커밋

```bash
git commit -m "메시지"        # 커밋
git commit -am "메시지"       # 수정된 파일 스테이징 + 커밋
```

## 5️⃣ 브랜치 관리

```bash
git branch                 # 브랜치 목록 확인
git branch <name>          # 새 브랜치 생성
git checkout <branch>      # 브랜치 전환
git switch <branch>        # 브랜치 전환 (checkout 대체)
git merge <branch>         # 다른 브랜치 병합
git branch -d <branch>     # 브랜치 삭제
```

## 6️⃣ 원격 저장소

```bash
git remote -v              # 원격 저장소 확인
git remote add origin <url> # 원격 저장소 추가
git fetch origin           # 원격 저장소 최신 정보 가져오기
git pull origin <branch>   # 원격 브랜치 병합
git push origin <branch>   # 로컬 브랜치 원격 저장소로 푸시
git push origin --delete <branch> # 원격 브랜치 삭제
```

## 7️⃣ 병합 충돌 관리

```bash
git merge <branch>          # 병합 시 충돌 발생 가능
git merge --abort           # 병합 취소
git rebase <branch>         # 커밋 재배치
git rebase --abort          # 리베이스 취소
```

## 8️⃣ 되돌리기

```bash
git reset --hard <commit>   # 특정 커밋으로 되돌림 (주의: 로컬 변경사항 삭제)
git reset --soft <commit>   # 커밋 되돌리지만 변경사항 유지
git checkout -- <file>      # 특정 파일 변경사항 버리기
git revert <commit>         # 커밋을 되돌리는 새 커밋 생성
```

## 9️⃣ 기타

```bash
git stash                   # 임시로 변경사항 저장
git stash pop               # 저장했던 변경사항 복원
git tag <name>              # 태그 생성
git show <commit/tag>       # 커밋/태그 내용 확인
```


