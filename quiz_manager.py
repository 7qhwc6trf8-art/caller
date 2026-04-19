import json
import random
from dataclasses import dataclass, field

@dataclass
class QuizSession:
    chat_id: int
    questions: list
    current_index: int = 0
    scores: dict[int, int] = field(default_factory=dict) # user_id: count
    is_active: bool = True

class QuizManager:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.sessions: dict[int, QuizSession] = {}
        self.master_questions = self._load_questions()

    def _load_questions(self):
        with open(self.file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data['questions']  # Navigates to the list under 'questions'

    def start_new_session(self, chat_id: int):
        questions = self.master_questions.copy()
        random.shuffle(questions)
        self.sessions[chat_id] = QuizSession(chat_id=chat_id, questions=questions)
        return self.sessions[chat_id]

    def get_session(self, chat_id: int) -> QuizSession:
        return self.sessions.get(chat_id)

    def stop_session(self, chat_id: int):
        if chat_id in self.sessions:
            del self.sessions[chat_id]